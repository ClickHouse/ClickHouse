from optparse import OptionParser
import argparse

import re
import sys

def log_to_rows(filename, pattern_select, pattern_sec, pattern_ignore):
    sec_matcher = re.compile(pattern_sec)
    select_matcher = re.compile(pattern_select, re.IGNORECASE);
    ignore_matcher = re.compile(pattern_ignore)

    f = open(filename, 'r');

    query = ''
    for line in f:
        if ignore_matcher.match(line):
            continue

        m = select_matcher.search(line)
        if m :
            if line != query:
                query = line
                sys.stdout.write("\n")

        m = sec_matcher.search(line)
        if m:
            sys.stdout.write(m.group(1) + " " )            
    print


def process_log(filename, pattern_select, pattern_sec, pattern_ignore):
    sec_matcher = re.compile(pattern_sec)
    select_matcher = re.compile(pattern_select, re.IGNORECASE);
    ignore_matcher = re.compile(pattern_ignore)

    f = open(filename, 'r');

    query = ''
    for line in f:
        if ignore_matcher.match(line):
            continue

        m = select_matcher.search(line)
        if m :
            if line != query:
                sys.stdout.flush()
                query = line
                print "\n\n"
                print query 

        m = sec_matcher.search(line)
        if m:
            sys.stdout.write(m.group(1) + " " )            
    print

def main():
    parser = argparse.ArgumentParser(description="Process log files form different databases")
    parser.add_argument('log_file', metavar = 'log_file', help = 'database log file')
    parser.add_argument('db_name', metavar = 'db_name', help = ' database name one of clickhouse, vertica, infinidb, monetdb (... more later)')
    args = parser.parse_args()

    log_file = args.log_file
    db_name = args.db_name

    sec_pattern = ''
    select_pattern = ''
    ignore_pattern = ''
    if db_name == 'clickhouse':
            sec_pattern = '(\d+.\d{3}) sec'
            select_pattern = 'select '
            ignore_pattern = ':\).*'
    elif db_name == 'vertica' :
            sec_pattern = '(\d+,\d+) ms\.'
            select_pattern = 'select '
            ignore_pattern = '(.*dbadmin=>|query:|.*Timing is on\.).*'            
    elif db_name == 'infinidb' :
            sec_pattern = '(\d+.\d+) sec'
            select_pattern = 'query: select '
            ignore_pattern = '#'            
    elif db_name == 'monetdb' :
            sec_pattern = '(\d+[m. ]*\d+[ms]+)\)'
            select_pattern = 'query: select '
            ignore_pattern = '#'            
    else:
        sys.exit("unknown db_name")
    
    process_log(log_file, select_pattern, sec_pattern, ignore_pattern )
    log_to_rows(log_file, select_pattern, sec_pattern, ignore_pattern )

main()
