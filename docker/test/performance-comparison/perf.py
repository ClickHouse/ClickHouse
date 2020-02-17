#!/usr/bin/python3

import os
import sys
import itertools
import clickhouse_driver
import xml.etree.ElementTree as et
import argparse
import pprint
import time
import traceback

stage_start_seconds = time.perf_counter()

def report_stage_end(stage_name):
    global stage_start_seconds
    print('{}\t{}'.format(stage_name, time.perf_counter() - stage_start_seconds))
    stage_start_seconds = time.perf_counter()

report_stage_end('start')

parser = argparse.ArgumentParser(description='Run performance test.')
# Explicitly decode files as UTF-8 because sometimes we have Russian characters in queries, and LANG=C is set.
parser.add_argument('file', metavar='FILE', type=argparse.FileType('r', encoding='utf-8'), nargs=1, help='test description file')
parser.add_argument('--host', nargs='*', default=['127.0.0.1', '127.0.0.1'])
parser.add_argument('--port', nargs='*', default=[9001, 9002])
parser.add_argument('--runs', type=int, default=int(os.environ.get('CHPC_RUNS', 7)))
args = parser.parse_args()

tree = et.parse(args.file[0])
root = tree.getroot()

# Check main metric
main_metric_element = root.find('main_metric/*')
if main_metric_element is not None and main_metric_element.tag != 'min_time':
    raise Exception('Only the min_time main metric is supported. This test uses \'{}\''.format(main_metric_element.tag))

# FIXME another way to detect infinite tests. They should have an appropriate main_metric but sometimes they don't.
infinite_sign = root.find('.//average_speed_not_changing_for_ms')
if infinite_sign is not None:
    raise Exception('Looks like the test is infinite (sign 1)')

# Open connections
servers = [{'host': host, 'port': port} for (host, port) in zip(args.host, args.port)]
connections = [clickhouse_driver.Client(**server) for server in servers]

report_stage_end('connect')

# Check tables that should exist
tables = [e.text for e in root.findall('preconditions/table_exists')]
for t in tables:
    for c in connections:
        res = c.execute("show create table {}".format(t))

# Apply settings
settings = root.findall('settings/*')
for c in connections:
    for s in settings:
        c.execute("set {} = '{}'".format(s.tag, s.text))

report_stage_end('preconditions')

# Process substitutions
subst_elems = root.findall('substitutions/substitution')

parameter_keys = []         # ['table',                   'limit'    ]
parameter_value_arrays = [] # [['hits_100m', 'hits_10m'], ['1', '10']]
parameter_combinations = [] # [{table: hits_100m, limit: 1}, ...]
for se in subst_elems:
    parameter_keys.append(se.find('name').text)
    parameter_value_arrays.append([v.text for v in se.findall('values/value')])
parameter_combinations = [dict(zip(parameter_keys, parameter_combination)) for parameter_combination in itertools.product(*parameter_value_arrays)]

def substitute_parameters(query_templates, parameter_combinations):
    return list(set([template.format(**parameters) for template, parameters in itertools.product(query_templates, parameter_combinations)]))

report_stage_end('substitute')

# Run drop queries, ignoring errors
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates, parameter_combinations)
for c in connections:
    for q in drop_queries:
        try:
            c.execute(q)
        except:
            traceback.print_exc()
            pass

# Run create queries
create_query_templates = [q.text for q in root.findall('create_query')]
create_queries = substitute_parameters(create_query_templates, parameter_combinations)
for c in connections:
    for q in create_queries:
        c.execute(q)

# Run fill queries
fill_query_templates = [q.text for q in root.findall('fill_query')]
fill_queries = substitute_parameters(fill_query_templates, parameter_combinations)
for c in connections:
    for q in fill_queries:
        c.execute(q)

report_stage_end('fill')

# Run test queries
def tsv_escape(s):
    return s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r','')

test_query_templates = [q.text for q in root.findall('query')]
test_queries = substitute_parameters(test_query_templates, parameter_combinations)

report_stage_end('substitute2')

for q in test_queries:
    # Prewarm: run once on both servers. Helps to bring the data into memory,
    # precompile the queries, etc.
    for conn_index, c in enumerate(connections):
        res = c.execute(q)
        print('prewarm\t' + tsv_escape(q) + '\t' + str(conn_index) + '\t' + str(c.last_query.elapsed))

    # Now, perform measured runs.
    # Track the time spent by the client to process this query, so that we can notice
    # out the queries that take long to process on the client side, e.g. by sending
    # excessive data.
    start_seconds = time.perf_counter()
    server_seconds = 0
    for run in range(0, args.runs):
        for conn_index, c in enumerate(connections):
            res = c.execute(q)
            print('query\t' + tsv_escape(q) + '\t' + str(run) + '\t' + str(conn_index) + '\t' + str(c.last_query.elapsed))
            server_seconds += c.last_query.elapsed

    client_seconds = time.perf_counter() - start_seconds
    print('client-time\t{}\t{}\t{}'.format(tsv_escape(q), client_seconds, server_seconds))

report_stage_end('benchmark')

# Run drop queries
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates, parameter_combinations)
for c in connections:
    for q in drop_queries:
        c.execute(q)

report_stage_end('drop')
