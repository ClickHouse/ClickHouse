from optparse import OptionParser
import argparse

import re
import sys
    
def log_to_rows(filename, pattern_select, time_pattern, pattern_ignore):
    time_matcher = re.compile(time_pattern)
    select_matcher = re.compile(pattern_select, re.IGNORECASE);
    ignore_matcher = re.compile(pattern_ignore)

    f = open(filename, 'r');

    query = ''
    raw_time = ''
    for line in f:
        if ignore_matcher.match(line):
            continue

        m = select_matcher.search(line)
        if m :
            if line != query:
                query = line
                sys.stdout.write("\n")
                raw_time = raw_time + "\n"

        m = time_matcher.search(line)
        if m:
            sec = 0
            minute = 0
            ms = 0
            if 'min' in m.groupdict() and m.group('min'):
                minute = float(m.group('min').replace(',','.'))
            if 'sec' in m.groupdict() and m.group('sec'):
                sec = float(m.group('sec').replace(',','.'))
            if 'ms' in m.groupdict() and m.group('ms'):
                ms = float(m.group('ms').replace(',', '.'))

            sys.stdout.write( str(minute*60 + sec + ms/1000.)  + " " )    
            raw_time = raw_time + " | " + m.group('time')

    print
    print " =======raw time====== \n" + raw_time


def process_log(filename, pattern_select, time_pattern, pattern_ignore, error_pattern):
    time_matcher = re.compile(time_pattern)
    select_matcher = re.compile(pattern_select, re.IGNORECASE);
    ignore_matcher = re.compile(pattern_ignore)
    error_matcher = re.compile(error_pattern, re.IGNORECASE)

    f = open(filename, 'r');

    query = ''
    for line in f:
        if error_matcher.match(line):
            print line
            continue

        if ignore_matcher.match(line):
            continue

        m = select_matcher.search(line)
        if m :
            if line != query:
                sys.stdout.flush()
                query = line
                print "\n\n"
                print query 

        m = time_matcher.search(line)
        if m:
            sys.stdout.write(m.group('time') + " " )

def main():
    parser = argparse.ArgumentParser(description="Process log files form different databases")
    parser.add_argument('log_file', metavar = 'log_file', help = 'database log file')
    parser.add_argument('db_name', metavar = 'db_name', help = ' database name one of clickhouse, vertica, infinidb, monetdb, infobright, hive (... more later)')
    args = parser.parse_args()

    log_file = args.log_file
    db_name = args.db_name

    time_pattern = ''
    select_pattern = r'query: select '
    ignore_pattern = r'#'
    error_pattern = r'error .*'
    if db_name == 'clickhouse':
        time_pattern = r'(?P<time>(?P<sec>\d+.\d{3}) sec\.)'
        select_pattern = r'query\: select '
        ignore_pattern = r':\).*'
    elif db_name == 'vertica' :
        time_pattern = r'(?P<time>(?P<ms>\d+.\d+) ms\.)'
        select_pattern = r'select '
        ignore_pattern = r'(.*dbadmin=>|query:|.*Timing is on\.).*'            
    elif db_name == 'infinidb' :
        time_pattern = r'(?P<time>(?:(?P<min>\d+) min )?(?P<sec>\d+.\d+) sec)'
        ignore_pattern = r'Query OK, 0 rows affected \(0\.00 sec\)'
    elif db_name == 'monetdb' :
        time_pattern = r'tuples? \((?P<time>(?:(?P<min>\d+)m )?(?:(?P<sec>\d+.?\d+)s)?(?:(?P<ms>\d+.\d+)ms)?)\)'
    elif db_name == 'infobright' :
        time_pattern = r'(?P<time>(?:(?P<min>\d+) min ){0,1}(?P<sec>\d+.\d+) sec)'
    elif db_name == 'hive':
        time_pattern = r'Time taken\: (?P<time>(?:(?P<sec>\d+.?\d+) seconds))'
        error_pattern = r'failed\: .*'
    else:
        sys.exit("unknown db_name")
    
    process_log(log_file, select_pattern, time_pattern, ignore_pattern, error_pattern )
    log_to_rows(log_file, select_pattern, time_pattern, ignore_pattern )

main()
