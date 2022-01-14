#!/usr/bin/env python3

import argparse
import clickhouse_driver
import itertools
import functools
import math
import os
import pprint
import random
import re
import statistics
import string
import sys
import time
import traceback
import logging
import xml.etree.ElementTree as et
from threading import Thread
from scipy import stats

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level='WARNING')

total_start_seconds = time.perf_counter()
stage_start_seconds = total_start_seconds

def reportStageEnd(stage):
    global stage_start_seconds, total_start_seconds

    current = time.perf_counter()
    print(f'stage\t{stage}\t{current - stage_start_seconds:.3f}\t{current - total_start_seconds:.3f}')
    stage_start_seconds = current


def tsv_escape(s):
    return s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r','')


parser = argparse.ArgumentParser(description='Run performance test.')
# Explicitly decode files as UTF-8 because sometimes we have Russian characters in queries, and LANG=C is set.
parser.add_argument('file', metavar='FILE', type=argparse.FileType('r', encoding='utf-8'), nargs=1, help='test description file')
parser.add_argument('--host', nargs='*', default=['localhost'], help="Space-separated list of server hostname(s). Corresponds to '--port' options.")
parser.add_argument('--port', nargs='*', default=[9000], help="Space-separated list of server port(s). Corresponds to '--host' options.")
parser.add_argument('--runs', type=int, default=1, help='Number of query runs per server.')
parser.add_argument('--max-queries', type=int, default=None, help='Test no more than this number of queries, chosen at random.')
parser.add_argument('--queries-to-run', nargs='*', type=int, default=None, help='Space-separated list of indexes of queries to test.')
parser.add_argument('--max-query-seconds', type=int, default=15, help='For how many seconds at most a query is allowed to run. The script finishes with error if this time is exceeded.')
parser.add_argument('--profile-seconds', type=int, default=0, help='For how many seconds to profile a query for which the performance has changed.')
parser.add_argument('--long', action='store_true', help='Do not skip the tests tagged as long.')
parser.add_argument('--print-queries', action='store_true', help='Print test queries and exit.')
parser.add_argument('--print-settings', action='store_true', help='Print test settings and exit.')
parser.add_argument('--keep-created-tables', action='store_true', help="Don't drop the created tables after the test.")
parser.add_argument('--use-existing-tables', action='store_true', help="Don't create or drop the tables, use the existing ones instead.")
args = parser.parse_args()

reportStageEnd('start')

test_name = os.path.splitext(os.path.basename(args.file[0].name))[0]

tree = et.parse(args.file[0])
root = tree.getroot()

reportStageEnd('parse')

# Process query parameters
subst_elems = root.findall('substitutions/substitution')
available_parameters = {} # { 'table': ['hits_10m', 'hits_100m'], ... }
for e in subst_elems:
    name = e.find('name').text
    values = [v.text for v in e.findall('values/value')]
    if not values:
        raise Exception(f'No values given for substitution {{{name}}}')

    available_parameters[name] = values

# Takes parallel lists of templates, substitutes them with all combos of
# parameters. The set of parameters is determined based on the first list.
# Note: keep the order of queries -- sometimes we have DROP IF EXISTS
# followed by CREATE in create queries section, so the order matters.
def substitute_parameters(query_templates, other_templates = []):
    query_results = []
    other_results = [[]] * (len(other_templates))
    for i, q in enumerate(query_templates):
        # We need stable order of keys here, so that the order of substitutions
        # is always the same, and the query indexes are consistent across test
        # runs.
        keys = sorted(set(n for _, n, _, _ in string.Formatter().parse(q) if n))
        values = [available_parameters[k] for k in keys]
        combos = itertools.product(*values)
        for c in combos:
            with_keys = dict(zip(keys, c))
            query_results.append(q.format(**with_keys))
            for j, t in enumerate(other_templates):
                other_results[j].append(t[i].format(**with_keys))
    if len(other_templates):
        return query_results, other_results
    else:
        return query_results


# Build a list of test queries, substituting parameters to query templates,
# and reporting the queries marked as short.
test_queries = []
is_short = []
for e in root.findall('query'):
    new_queries, [new_is_short] = substitute_parameters([e.text], [[e.attrib.get('short', '0')]])
    test_queries += new_queries
    is_short += [eval(s) for s in new_is_short]

assert(len(test_queries) == len(is_short))

# If we're given a list of queries to run, check that it makes sense.
for i in args.queries_to_run or []:
    if i < 0 or i >= len(test_queries):
        print(f'There is no query no. {i} in this test, only [{0}-{len(test_queries) - 1}] are present')
        exit(1)

# If we're only asked to print the queries, do that and exit.
if args.print_queries:
    for i in args.queries_to_run or range(0, len(test_queries)):
        print(test_queries[i])
    exit(0)

# Print short queries
for i, s in enumerate(is_short):
    if s:
        print(f'short\t{i}')

# If we're only asked to print the settings, do that and exit. These are settings
# for clickhouse-benchmark, so we print them as command line arguments, e.g.
# '--max_memory_usage=10000000'.
if args.print_settings:
    for s in root.findall('settings/*'):
        print(f'--{s.tag}={s.text}')

    exit(0)

# Skip long tests
if not args.long:
    for tag in root.findall('.//tag'):
        if tag.text == 'long':
            print('skipped\tTest is tagged as long.')
            sys.exit(0)

# Print report threshold for the test if it is set.
ignored_relative_change = 0.05
if 'max_ignored_relative_change' in root.attrib:
    ignored_relative_change = float(root.attrib["max_ignored_relative_change"])
    print(f'report-threshold\t{ignored_relative_change}')

reportStageEnd('before-connect')

# Open connections
servers = [{'host': host or args.host[0], 'port': port or args.port[0]} for (host, port) in itertools.zip_longest(args.host, args.port)]
# Force settings_is_important to fail queries on unknown settings.
all_connections = [clickhouse_driver.Client(**server, settings_is_important=True) for server in servers]

for i, s in enumerate(servers):
    print(f'server\t{i}\t{s["host"]}\t{s["port"]}')

reportStageEnd('connect')

if not args.use_existing_tables:
    # Run drop queries, ignoring errors. Do this before all other activity,
    # because clickhouse_driver disconnects on error (this is not configurable),
    # and the new connection loses the changes in settings.
    drop_query_templates = [q.text for q in root.findall('drop_query')]
    drop_queries = substitute_parameters(drop_query_templates)
    for conn_index, c in enumerate(all_connections):
        for q in drop_queries:
            try:
                c.execute(q)
                print(f'drop\t{conn_index}\t{c.last_query.elapsed}\t{tsv_escape(q)}')
            except:
                pass

    reportStageEnd('drop-1')

# Apply settings.
settings = root.findall('settings/*')
for conn_index, c in enumerate(all_connections):
    for s in settings:
        # requires clickhouse-driver >= 1.1.5 to accept arbitrary new settings
        # (https://github.com/mymarilyn/clickhouse-driver/pull/142)
        c.settings[s.tag] = s.text

reportStageEnd('settings')

# Check tables that should exist. If they don't exist, just skip this test.
tables = [e.text for e in root.findall('preconditions/table_exists')]
for t in tables:
    for c in all_connections:
        try:
            res = c.execute("select 1 from {} limit 1".format(t))
        except:
            exception_message = traceback.format_exception_only(*sys.exc_info()[:2])[-1]
            skipped_message = ' '.join(exception_message.split('\n')[:2])
            print(f'skipped\t{tsv_escape(skipped_message)}')
            sys.exit(0)

reportStageEnd('preconditions')

if not args.use_existing_tables:
    # Run create and fill queries. We will run them simultaneously for both
    # servers, to save time. The weird XML search + filter is because we want to
    # keep the relative order of elements, and etree doesn't support the
    # appropriate xpath query.
    create_query_templates = [q.text for q in root.findall('./*')
        if q.tag in ('create_query', 'fill_query')]
    create_queries = substitute_parameters(create_query_templates)

    # Disallow temporary tables, because the clickhouse_driver reconnects on
    # errors, and temporary tables are destroyed. We want to be able to continue
    # after some errors.
    for q in create_queries:
        if re.search('create temporary table', q, flags=re.IGNORECASE):
            print(f"Temporary tables are not allowed in performance tests: '{q}'",
                file = sys.stderr)
            sys.exit(1)

    def do_create(connection, index, queries):
        for q in queries:
            connection.execute(q)
            print(f'create\t{index}\t{connection.last_query.elapsed}\t{tsv_escape(q)}')

    threads = [
        Thread(target = do_create, args = (connection, index, create_queries))
        for index, connection in enumerate(all_connections)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    reportStageEnd('create')

# By default, test all queries.
queries_to_run = range(0, len(test_queries))

if args.max_queries:
    # If specified, test a limited number of queries chosen at random.
    queries_to_run = random.sample(range(0, len(test_queries)), min(len(test_queries), args.max_queries))

if args.queries_to_run:
    # Run the specified queries.
    queries_to_run = args.queries_to_run

# Run test queries.
profile_total_seconds = 0
for query_index in queries_to_run:
    q = test_queries[query_index]
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
    # A query might not run on the old server if it uses a function added in the
    # new one. We want to run them on the new server only, so that the PR author
    # can ensure that the test works properly. Remember the errors we had on
    # each server.
    query_error_on_connection = [None] * len(all_connections);
    for conn_index, c in enumerate(all_connections):
        try:
            prewarm_id = f'{query_prefix}.prewarm0'

            try:
                # During the warmup runs, we will also:
                # * detect queries that are exceedingly long, to fail fast,
                # * collect profiler traces, which might be helpful for analyzing
                #   test coverage. We disable profiler for normal runs because
                #   it makes the results unstable.
                res = c.execute(q, query_id = prewarm_id,
                    settings = {'max_execution_time': args.max_query_seconds,
                        'query_profiler_real_time_period_ns': 10000000})
            except clickhouse_driver.errors.Error as e:
                # Add query id to the exception to make debugging easier.
                e.args = (prewarm_id, *e.args)
                e.message = prewarm_id + ': ' + e.message
                raise

            print(f'prewarm\t{query_index}\t{prewarm_id}\t{conn_index}\t{c.last_query.elapsed}')
        except KeyboardInterrupt:
            raise
        except:
            # FIXME the driver reconnects on error and we lose settings, so this
            # might lead to further errors or unexpected behavior.
            query_error_on_connection[conn_index] = traceback.format_exc();
            continue

    # Report all errors that ocurred during prewarm and decide what to do next.
    # If prewarm fails for the query on all servers -- skip the query and
    # continue testing the next query.
    # If prewarm fails on one of the servers, run the query on the rest of them.
    no_errors = []
    for i, e in enumerate(query_error_on_connection):
        if e:
            print(e, file = sys.stderr)
        else:
            no_errors.append(i)

    if len(no_errors) == 0:
        continue
    elif len(no_errors) < len(all_connections):
        print(f'partial\t{query_index}\t{no_errors}')

    this_query_connections = [all_connections[index] for index in no_errors]

    # Now, perform measured runs.
    # Track the time spent by the client to process this query, so that we can
    # notice the queries that take long to process on the client side, e.g. by
    # sending excessive data.
    start_seconds = time.perf_counter()
    server_seconds = 0
    profile_seconds = 0
    run = 0

    # Arrays of run times for each connection.
    all_server_times = []
    for conn_index, c in enumerate(this_query_connections):
        all_server_times.append([])

    while True:
        run_id = f'{query_prefix}.run{run}'

        for conn_index, c in enumerate(this_query_connections):
            try:
                res = c.execute(q, query_id = run_id, settings = {'max_execution_time': args.max_query_seconds})
            except clickhouse_driver.errors.Error as e:
                # Add query id to the exception to make debugging easier.
                e.args = (run_id, *e.args)
                e.message = run_id + ': ' + e.message
                raise

            elapsed = c.last_query.elapsed
            all_server_times[conn_index].append(elapsed)

            server_seconds += elapsed
            print(f'query\t{query_index}\t{run_id}\t{conn_index}\t{elapsed}')

            if elapsed > args.max_query_seconds:
                # Stop processing pathologically slow queries, to avoid timing out
                # the entire test task. This shouldn't really happen, so we don't
                # need much handling for this case and can just exit.
                print(f'The query no. {query_index} is taking too long to run ({elapsed} s)', file=sys.stderr)
                exit(2)

        # Be careful with the counter, after this line it's the next iteration
        # already.
        run += 1

        # Try to run any query for at least the specified number of times,
        # before considering other stop conditions.
        if run < args.runs:
            continue

        # For very short queries we have a special mode where we run them for at
        # least some time. The recommended lower bound of run time for "normal"
        # queries is about 0.1 s, and we run them about 10 times, giving the
        # time per query per server of about one second. Run "short" queries
        # for longer time, because they have a high percentage of overhead and
        # might give less stable results.
        if is_short[query_index]:
            if server_seconds >= 8 * len(this_query_connections):
                break
            # Also limit the number of runs, so that we don't go crazy processing
            # the results -- 'eqmed.sql' is really suboptimal.
            if run >= 500:
                break
        else:
            if run >= args.runs:
                break

    client_seconds = time.perf_counter() - start_seconds
    print(f'client-time\t{query_index}\t{client_seconds}\t{server_seconds}')

    # Run additional profiling queries to collect profile data, but only if test times appeared to be different.
    # We have to do it after normal runs because otherwise it will affect test statistics too much
    if len(all_server_times) != 2:
        continue

    if len(all_server_times[0]) < 3:
        # Don't fail if for some reason there are not enough measurements.
        continue

    pvalue = stats.ttest_ind(all_server_times[0], all_server_times[1], equal_var = False).pvalue
    median = [statistics.median(t) for t in all_server_times]
    # Keep this consistent with the value used in report. Should eventually move
    # to (median[1] - median[0]) / min(median), which is compatible with "times"
    # difference we use in report (max(median) / min(median)).
    relative_diff = (median[1] - median[0]) / median[0]
    print(f'diff\t{query_index}\t{median[0]}\t{median[1]}\t{relative_diff}\t{pvalue}')
    if abs(relative_diff) < ignored_relative_change or pvalue > 0.05:
        continue

    # Perform profile runs for fixed amount of time. Don't limit the number
    # of runs, because we also have short queries.
    profile_start_seconds = time.perf_counter()
    run = 0
    while time.perf_counter() - profile_start_seconds < args.profile_seconds:
        run_id = f'{query_prefix}.profile{run}'

        for conn_index, c in enumerate(this_query_connections):
            try:
                res = c.execute(q, query_id = run_id, settings = {'query_profiler_real_time_period_ns': 10000000})
                print(f'profile\t{query_index}\t{run_id}\t{conn_index}\t{c.last_query.elapsed}')
            except clickhouse_driver.errors.Error as e:
                # Add query id to the exception to make debugging easier.
                e.args = (run_id, *e.args)
                e.message = run_id + ': ' + e.message
                raise

        run += 1

    profile_total_seconds += time.perf_counter() - profile_start_seconds

print(f'profile-total\t{profile_total_seconds}')

reportStageEnd('run')

# Run drop queries
if not args.keep_created_tables and not args.use_existing_tables:
    drop_queries = substitute_parameters(drop_query_templates)
    for conn_index, c in enumerate(all_connections):
        for q in drop_queries:
            c.execute(q)
            print(f'drop\t{conn_index}\t{c.last_query.elapsed}\t{tsv_escape(q)}')

    reportStageEnd('drop-2')
