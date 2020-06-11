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

def tsv_escape(s):
    return s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r','')

parser = argparse.ArgumentParser(description='Run performance test.')
# Explicitly decode files as UTF-8 because sometimes we have Russian characters in queries, and LANG=C is set.
parser.add_argument('file', metavar='FILE', type=argparse.FileType('r', encoding='utf-8'), nargs=1, help='test description file')
parser.add_argument('--host', nargs='*', default=['localhost'], help="Server hostname(s). Corresponds to '--port' options.")
parser.add_argument('--port', nargs='*', default=[9000], help="Server port(s). Corresponds to '--host' options.")
parser.add_argument('--runs', type=int, default=int(os.environ.get('CHPC_RUNS', 17)), help='Number of query runs per server. Defaults to CHPC_RUNS environment variable.')
parser.add_argument('--long', action='store_true', help='Do not skip the tests tagged as long.')
parser.add_argument('--print', action='store_true', help='Print test queries and exit.')
args = parser.parse_args()

test_name = os.path.splitext(os.path.basename(args.file[0].name))[0]

tree = et.parse(args.file[0])
root = tree.getroot()

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

# Build a list of test queries, processing all substitutions
test_query_templates = [q.text for q in root.findall('query')]
test_queries = substitute_parameters(test_query_templates)

# If we're only asked to print the queries, do that and exit
if args.print:
    for q in test_queries:
        print(q)
    exit(0)

# Skip long tests
if not args.long:
    for tag in root.findall('.//tag'):
        if tag.text == 'long':
            print('skipped\tTest is tagged as long.')
            sys.exit(0)

# Check main metric to detect infinite tests. We shouldn't have such tests anymore,
# but we did in the past, and it is convenient to be able to process old tests.
main_metric_element = root.find('main_metric/*')
if main_metric_element is not None and main_metric_element.tag != 'min_time':
    raise Exception('Only the min_time main metric is supported. This test uses \'{}\''.format(main_metric_element.tag))

# Another way to detect infinite tests. They should have an appropriate main_metric
# but sometimes they don't.
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

# Apply settings.
# If there are errors, report them and continue -- maybe a new test uses a setting
# that is not in master, but the queries can still run. If we have multiple
# settings and one of them throws an exception, all previous settings for this
# connection will be reset, because the driver reconnects on error (not
# configurable). So the end result is uncertain, but hopefully we'll be able to
# run at least some queries.
settings = root.findall('settings/*')
for c in connections:
    for s in settings:
        try:
            c.execute("set {} = '{}'".format(s.tag, s.text))
        except:
            print(traceback.format_exc(), file=sys.stderr)

# Check tables that should exist. If they don't exist, just skip this test.
tables = [e.text for e in root.findall('preconditions/table_exists')]
for t in tables:
    for c in connections:
        try:
            res = c.execute("select 1 from {} limit 1".format(t))
        except:
            exception_message = traceback.format_exception_only(*sys.exc_info()[:2])[-1]
            skipped_message = ' '.join(exception_message.split('\n')[:2])
            print(f'skipped\t{tsv_escape(skipped_message)}')
            sys.exit(0)

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

# Run test queries
for query_index, q in enumerate(test_queries):
    query_prefix = f'{test_name}.query{query_index}'

    # We have some crazy long queries (about 100kB), so trim them to a sane
    # length. This means we can't use query text as an identifier and have to
    # use the test name + the test-wide query index.
    query_display_name = q
    if len(query_display_name) > 1000:
        query_display_name = f'{query_display_name[:1000]}...({query_index})'

    print(f'display-name\t{query_index}\t{tsv_escape(query_display_name)}')

    # Prewarm: run once on both servers. Helps to bring the data into memory,
    # precompile the queries, etc.
    try:
        for conn_index, c in enumerate(connections):
            prewarm_id = f'{query_prefix}.prewarm0'
            res = c.execute(q, query_id = prewarm_id)
            print(f'prewarm\t{query_index}\t{prewarm_id}\t{conn_index}\t{c.last_query.elapsed}')
    except KeyboardInterrupt:
        raise
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

# Run drop queries
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates)
for c in connections:
    for q in drop_queries:
        c.execute(q)
