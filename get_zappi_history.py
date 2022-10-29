#!/usr/bin/env python3

"""Show the boost status"""

import getopt
import time
import sys
import json
import logging
import os.path
import logging.handlers
import yaml
import tabulate

import mec.zp
import mec.power_meter

# This needs to have debugging disabled.

FIELD_NAMES = {'gep': 'Generated',
               'gen': 'Generated Negative',
               'h1d': 'Phase 1 diverted',
               'h2d': 'Phase 2 diverted',
               'h3d': 'Phase 3 diverted',
               'h1b': 'Zappi imported',
               'imp': 'Imported',
               'exp': 'Exported'}

RC_FILE = '~/.zappirc'

DELAY = 60

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

class Day():

    def __init__(self, year, month, day):
        self.tm_year = year
        self.tm_mon = month
        self.tm_mday = day


def setup_logging(debug):
    """Configure global logging state"""

    if not debug:
        return
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Log info to stdout natively.
    channel = logging.StreamHandler()
    oformat = logging.Formatter()
    channel.setLevel(logging.DEBUG)
    channel.setFormatter(oformat)
    root.addHandler(channel)

    # Log debug to file, and add prefix.
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    log_file = os.path.join(LOG_DIR, 'myenergi.log')
    channel = logging.handlers.TimedRotatingFileHandler(
        log_file, encoding='utf-8')
    my_pid = os.getpid()
    mformat = '%(asctime)s - {} - %(name)s - %(levelname)s - %(message)s'.format(my_pid)
    oformat = logging.Formatter(mformat)
    channel.setLevel(logging.DEBUG)
    channel.setFormatter(oformat)
    root.addHandler(channel)


# Get logging handle for this file.
log = logging.getLogger('run_zappi')


def load_config(debug=True):
    """Load the config file and return dict"""
    setup_logging(debug)
    with open(os.path.expanduser(RC_FILE), 'r') as ofh:
        return yaml.safe_load(ofh)


show_headers = True


def main():
    """Main"""
    global show_headers

    args = ['per-minute',
            'totals',
            'day=',
            'month=',
            'year=',
            'show-month',
            'json']
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', args)
    except getopt.GetoptError:
        print('Unknown options')
        print(args)
        sys.exit(2)

    hourly = True
    totals = False
    use_json = False

    today = time.localtime()
    day = Day(today.tm_year, today.tm_mon, today.tm_mday)
    show_month = False

    for opt, value in opts:
        if opt == '--per-minute':
            hourly = False
        elif opt == '--totals':
            totals = True
        elif opt == '--day':
            day.tm_mday = value
        elif opt == '--month':
            day.tm_mon = value
        elif opt == '--year':
            day.tm_year = value
        elif opt == '--show-month':
            show_month = True
        elif opt == '--json':
            use_json = True

    config = load_config(debug=True)

    server_conn = mec.zp.MyEnergiHost(config['username'], config['password'])
    server_conn.refresh()

    jout = {}

    # The Zappi V2.
    for zappi in server_conn.state.zappi_list():

        show_headers = True

        if use_json:
            (header, _, totals) = load_day(server_conn,
                                           zappi.sno,
                                           day,
                                           True,
                                           True,
                                           True)
            raw = {}
            for head in header:
                if not totals[0] or head == 'Time':
                    totals.pop(0)
                    continue
                raw[head] = totals.pop(0)
            jout[zappi.sno] = raw

        elif show_month:
            all_data = []
            for dom in range(1, day.tm_mday + 1):
                print('Day {}'.format(dom))
                day.tm_mday = dom
                (headers, _, totals) = load_day(server_conn,
                                                zappi.sno,
                                                day,
                                                hourly,
                                                totals,
                                                use_json)
                all_data.append(totals)
            print(tabulate.tabulate(all_data, headers=headers))
        else:
            load_day(server_conn, zappi.sno, day, hourly, totals, use_json)

    if use_json:
        print(json.dumps(jout, indent=4, sort_keys=True))


def load_day(server_conn, zid, day, hourly, totals, use_json):

    global show_headers

    if hourly:
        res = server_conn.get_hour_data(zid, day=day)
        prev_sample_time = - 60 * 60
    else:
        res = server_conn.get_minute_data(zid, day=day)
        prev_sample_time = -60

    headers = ['imp', 'exp', 'gen', 'gep', 'h1d', 'h2d', 'h3d', 'h1b']
    table_headers = ['Time', 'Duration']
    data = []
    pm_totals = {}
    for key in headers:
        pm_totals[key] = mec.power_meter.PowerMeter()
        pm_totals[key].add_value(0, prev_sample_time)
        if key in FIELD_NAMES:
            table_headers.append(FIELD_NAMES[key])
        else:
            table_headers.append(key)
    for rec in res:
        row = []
        hour = 0
        minute = 0
        volts = 1
        if 'imp' in rec and 'nect1' in rec and rec['imp'] == rec['nect1']:
            del rec['nect1']
        if 'exp' in rec and 'pect1' in rec and rec['exp'] == rec['pect1']:
            del rec['pect1']
        if 'hr' in rec:
            hour = rec['hr']
            del rec['hr']
        if 'min' in rec:
            minute = rec['min']
            del rec['min']

        sample_time = ((hour * 60) + minute) * 60

        for key in ['dow', 'yr', 'mon', 'dom']:
            del rec[key]

        if 'v1' in rec:
            volts = rec['v1'] / 10
        for key in ['v1', 'frq']:
            if key in rec:
                del rec[key]

        row.append('{:02}:{:02}'.format(hour, minute))
        row.append(sample_time - prev_sample_time)

        for key in headers:
            if key in rec:
                value = rec[key]
                if hourly:
                    watts = value / (60 * 60)
                else:
                    watts = value / volts * 4
                row.append(int(watts))
                del rec[key]
            else:
                watts = 0
                row.append(None)
            pm_totals[key].add_value(watts, sample_time)
        prev_sample_time = sample_time

        if rec:
            print(rec)
        data.append(row)
    num_records = len(data)
    if not use_json:
        print('There are {} records'.format(num_records))
    if totals:
        data = []
    row = ['Totals', None]
    for key in headers:
        row.append(str(pm_totals[key]))
    data.append(row)

    if not use_json:
        if show_headers:
            print(tabulate.tabulate(data, headers=table_headers))
            show_headers = False
        else:
            print(tabulate.tabulate(data))
    return (table_headers, data, row)


if __name__ == '__main__':
    main()
