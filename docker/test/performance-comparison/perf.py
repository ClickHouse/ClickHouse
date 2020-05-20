#!/usr/bin/python3

import os
import sys
import itertools
import clickhouse_driver
import xml.etree.ElementTree as et
import argparse
import pprint
import string
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
parser.add_argument('--host', nargs='*', default=['localhost'], help="Server hostname(s). Corresponds to '--port' options.")
parser.add_argument('--port', nargs='*', default=[9000], help="Server port(s). Corresponds to '--host' options.")
parser.add_argument('--runs', type=int, default=int(os.environ.get('CHPC_RUNS', 13)), help='Number of query runs per server. Defaults to CHPC_RUNS environment variable.')
parser.add_argument('--no-long', type=bool, default=True, help='Skip the tests tagged as long.')
args = parser.parse_args()

test_name = os.path.splitext(os.path.basename(args.file[0].name))[0]

tree = et.parse(args.file[0])
root = tree.getroot()

# Skip long tests
for tag in root.findall('.//tag'):
    if tag.text == 'long':
        print('skipped\tTest is tagged as long.')
        sys.exit(0)

# Check main metric
main_metric_element = root.find('main_metric/*')
if main_metric_element is not None and main_metric_element.tag != 'min_time':
    raise Exception('Only the min_time main metric is supported. This test uses \'{}\''.format(main_metric_element.tag))

# FIXME another way to detect infinite tests. They should have an appropriate main_metric but sometimes they don't.
infinite_sign = root.find('.//average_speed_not_changing_for_ms')
if infinite_sign is not None:
    raise Exception('Looks like the test is infinite (sign 1)')

# Print report threshold for the test if it is set.
if 'max_ignored_relative_change' in root.attrib:
    print(f'report-threshold\t{root.attrib["max_ignored_relative_change"]}')

# Open connections
servers = [{'host': host, 'port': port} for (host, port) in zip(args.host, args.port)]
connections = [clickhouse_driver.Client(**server) for server in servers]

for s in servers:
    print('server\t{}\t{}'.format(s['host'], s['port']))

report_stage_end('connect')

# Process query parameters
subst_elems = root.findall('substitutions/substitution')
available_parameters = {} # { 'table': ['hits_10m', 'hits_100m'], ... }
for e in subst_elems:
    available_parameters[e.find('name').text] = [v.text for v in e.findall('values/value')]

# Take care to keep the order of queries -- sometimes we have DROP IF EXISTS
# followed by CREATE in create queries section, so the order matters.
def substitute_parameters(query_templates):
    result = []
    for q in query_templates:
        keys = set(n for _, n, _, _ in string.Formatter().parse(q) if n)
        values = [available_parameters[k] for k in keys]
        result.extend([
            q.format(**dict(zip(keys, values_combo)))
                for values_combo in itertools.product(*values)])
    return result

report_stage_end('substitute')

# Run drop queries, ignoring errors. Do this before all other activity, because
# clickhouse_driver disconnects on error (this is not configurable), and the new
# connection loses the changes in settings.
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates)
for c in connections:
    for q in drop_queries:
        try:
            c.execute(q)
        except:
            pass

report_stage_end('drop1')

# Apply settings
settings = root.findall('settings/*')
for c in connections:
    for s in settings:
        c.execute("set {} = '{}'".format(s.tag, s.text))

report_stage_end('settings')

# Check tables that should exist. If they don't exist, just skip this test.
tables = [e.text for e in root.findall('preconditions/table_exists')]
for t in tables:
    for c in connections:
        try:
            res = c.execute("select 1 from {} limit 1".format(t))
        except:
            print('skipped\t' + traceback.format_exception_only(*sys.exc_info()[:2])[-1])
            traceback.print_exc()
            sys.exit(0)

report_stage_end('preconditions')

# Run create queries
create_query_templates = [q.text for q in root.findall('create_query')]
create_queries = substitute_parameters(create_query_templates)
for c in connections:
    for q in create_queries:
        c.execute(q)

# Run fill queries
fill_query_templates = [q.text for q in root.findall('fill_query')]
fill_queries = substitute_parameters(fill_query_templates)
for c in connections:
    for q in fill_queries:
        c.execute(q)

report_stage_end('fill')

# Run test queries
def tsv_escape(s):
    return s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r','')

test_query_templates = [q.text for q in root.findall('query')]
test_queries = substitute_parameters(test_query_templates)

report_stage_end('substitute2')

for query_index, q in enumerate(test_queries):
    query_prefix = f'{test_name}.query{query_index}'

    # We have some crazy long queries (about 100kB), so trim them to a sane
    # length. This means we can't use query text as an identifier and have to
    # use the test name + the test-wide query index.
    query_display_name = q
    if len(query_display_name) > 1000:
        query_display_name = f'{query_display_name[:1000]}...({i})'

    print(f'display-name\t{query_index}\t{tsv_escape(query_display_name)}')

    # Prewarm: run once on both servers. Helps to bring the data into memory,
    # precompile the queries, etc.
    try:
        for conn_index, c in enumerate(connections):
            prewarm_id = f'{query_prefix}.prewarm0'
            res = c.execute(q, query_id = prewarm_id)
            print(f'prewarm\t{query_index}\t{prewarm_id}\t{conn_index}\t{c.last_query.elapsed}')
    except:
        # If prewarm fails for some query -- skip it, and try to test the others.
        # This might happen if the new test introduces some function that the
        # old server doesn't support. Still, report it as an error.
        # FIXME the driver reconnects on error and we lose settings, so this might
        # lead to further errors or unexpected behavior.
        print(traceback.format_exc(), file=sys.stderr)
        continue

    # Now, perform measured runs.
    # Track the time spent by the client to process this query, so that we can notice
    # out the queries that take long to process on the client side, e.g. by sending
    # excessive data.
    start_seconds = time.perf_counter()
    server_seconds = 0
    for run in range(0, args.runs):
        run_id = f'{query_prefix}.run{run}'
        for conn_index, c in enumerate(connections):
            res = c.execute(q, query_id = run_id)
            print(f'query\t{query_index}\t{run_id}\t{conn_index}\t{c.last_query.elapsed}')
            server_seconds += c.last_query.elapsed

    client_seconds = time.perf_counter() - start_seconds
    print(f'client-time\t{query_index}\t{client_seconds}\t{server_seconds}')

report_stage_end('benchmark')

# Run drop queries
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates)
for c in connections:
    for q in drop_queries:
        c.execute(q)

report_stage_end('drop2')
