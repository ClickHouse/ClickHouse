#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


function configure
{
    # Use the new config for both servers, so that we can change it in a PR.
    rm right/config/config.d/text_log.xml ||:
    cp -rv right/config left ||:

    sed -i 's/<tcp_port>900./<tcp_port>9001/g' left/config/config.xml
    sed -i 's/<tcp_port>900./<tcp_port>9002/g' right/config/config.xml

    # Start a temporary server to rename the tables
    while killall clickhouse-server; do echo . ; sleep 1 ; done
    echo all killed

    set -m # Spawn temporary in its own process groups
    left/clickhouse-server --config-file=left/config/config.xml -- --path db0 --user_files_path db0/user_files &> setup-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid
    set +m
    while ! clickhouse-client --port 9001 --query "select 1" && kill -0 $left_pid ; do echo . ; sleep 1 ; done
    echo server for setup started

    clickhouse-client --port 9001 --query "create database test" ||:
    clickhouse-client --port 9001 --query "rename table datasets.hits_v1 to test.hits" ||:

    while killall clickhouse-server; do echo . ; sleep 1 ; done
    echo all killed

    # Make copies of the original db for both servers. Use hardlinks instead
    # of copying to save space. Before that, remove preprocessed configs and
    # system tables, because sharing them between servers with hardlinks may
    # lead to weird effects.
    rm -r left/db ||:
    rm -r right/db ||:
    rm -r db0/preprocessed_configs ||:
    rm -r db0/{data,metadata}/system ||:
    cp -al db0/ left/db/
    cp -al db0/ right/db/
}

function restart
{
    while killall clickhouse-server; do echo . ; sleep 1 ; done
    echo all killed

    set -m # Spawn servers in their own process groups

    left/clickhouse-server --config-file=left/config/config.xml -- --path left/db --user_files_path left/db/user_files &>> left-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid

    right/clickhouse-server --config-file=right/config/config.xml -- --path right/db --user_files_path right/db/user_files &>> right-server-log.log &
    right_pid=$!
    kill -0 $right_pid
    disown $right_pid

    set +m

    while ! clickhouse-client --port 9001 --query "select 1" && kill -0 $left_pid ; do echo . ; sleep 1 ; done
    echo left ok
    while ! clickhouse-client --port 9002 --query "select 1" && kill -0 $right_pid ; do echo . ; sleep 1 ; done
    echo right ok

    clickhouse-client --port 9001 --query "select * from system.tables where database != 'system'"
    clickhouse-client --port 9001 --query "select * from system.build_options"
    clickhouse-client --port 9002 --query "select * from system.tables where database != 'system'"
    clickhouse-client --port 9002 --query "select * from system.build_options"

    # Check again that both servers we started are running -- this is important
    # for running locally, when there might be some other servers started and we
    # will connect to them instead.
    kill -0 $left_pid
    kill -0 $right_pid
}

function run_tests
{
    # Just check that the script runs at all
    "$script_dir/perf.py" --help > /dev/null

    # Find the directory with test files.
    if [ -v CHPC_TEST_PATH ]
    then
        # Use the explicitly set path to directory with test files.
        test_prefix="$CHPC_TEST_PATH"
    elif [ "$PR_TO_TEST" = "0" ]
    then
        # When testing commits from master, use the older test files. This
        # allows the tests to pass even when we add new functions and tests for
        # them, that are not supported in the old revision.
        test_prefix=left/performance
    else
        # For PRs, use newer test files so we can test these changes.
        test_prefix=right/performance

        # If only the perf tests were changed in the PR, we will run only these
        # tests. The list of changed tests in changed-test.txt is prepared in
        # entrypoint.sh from git diffs, because it has the cloned repo.  Used
        # to use rsync for that but it was really ugly and not always correct
        # (e.g. when the reference SHA is really old and has some other
        # differences to the tested SHA, besides the one introduced by the PR).
        changed_test_files=$(sed "s/tests\/performance/${test_prefix//\//\\/}/" changed-tests.txt)
    fi

    # Determine which tests to run.
    if [ -v CHPC_TEST_GREP ]
    then
        # Run only explicitly specified tests, if any.
        test_files=$(ls "$test_prefix" | grep "$CHPC_TEST_GREP" | xargs -I{} -n1 readlink -f "$test_prefix/{}")
    elif [ "$changed_test_files" != "" ]
    then
        # Use test files that changed in the PR.
        test_files="$changed_test_files"
    else
        # The default -- run all tests found in the test dir.
        test_files=$(ls "$test_prefix"/*.xml)
    fi

    # Determine which concurrent benchmarks to run. For now, the only test
    # we run as a concurrent benchmark is 'website'. Run it as benchmark if we
    # are also going to run it as a normal test.
    for test in $test_files; do echo $test; done | sed -n '/website/p' > benchmarks-to-run.txt

    # Delete old report files.
    for x in {test-times,wall-clock-times}.tsv
    do
        rm -v "$x" ||:
        touch "$x"
    done

    # Randomize test order.
    test_files=$(for f in $test_files; do echo "$f"; done | sort -R)

    # Run the tests.
    test_name="<none>"
    for test in $test_files
    do
        # Check that both servers are alive, and restart them if they die.
        clickhouse-client --port 9001 --query "select 1 format Null" \
            || { echo $test_name >> left-server-died.log ; restart ; }
        clickhouse-client --port 9002 --query "select 1 format Null" \
            || { echo $test_name >> right-server-died.log ; restart ; }

        test_name=$(basename "$test" ".xml")
        echo test "$test_name"

        TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
        # the grep is to filter out set -x output and keep only time output
        { \
            time "$script_dir/perf.py" --host localhost localhost --port 9001 9002 \
                -- "$test" > "$test_name-raw.tsv" 2> "$test_name-err.log" ; \
        } 2>&1 >/dev/null | grep -v ^+ >> "wall-clock-times.tsv" \
            || echo "Test $test_name failed with error code $?" >> "$test_name-err.log"
    done

    unset TIMEFORMAT

    wait
}

# Run some queries concurrently and report the resulting TPS. This additional
# (relatively) short test helps detect concurrency-related effects, because the
# main performance comparison testing is done query-by-query.
function run_benchmark
{
    rm -rf benchmark ||:
    mkdir benchmark ||:

    # The list is built by run_tests.
    for file in $(cat benchmarks-to-run.txt)
    do
        name=$(basename "$file" ".xml")

        "$script_dir/perf.py" --print-queries "$file" > "benchmark/$name-queries.txt"
        "$script_dir/perf.py" --print-settings "$file" > "benchmark/$name-settings.txt"

        readarray -t settings < "benchmark/$name-settings.txt"
        command=(clickhouse-benchmark --concurrency 6 --cumulative --iterations 1000 --randomize 1 --delay 0 --continue_on_errors "${settings[@]}")

        "${command[@]}" --port 9001 --json "benchmark/$name-left.json" < "benchmark/$name-queries.txt"
        "${command[@]}" --port 9002 --json "benchmark/$name-right.json" < "benchmark/$name-queries.txt"
    done
}

function get_profiles_watchdog
{
    sleep 6000

    echo "The trace collection did not finish in time." >> profile-errors.log

    for pid in $(pgrep -f clickhouse)
    do
        gdb -p "$pid" --batch --ex "info proc all" --ex "thread apply all bt" --ex quit &> "$pid.gdb.log" &
    done
    wait

    for _ in {1..10}
    do
        if ! pkill -f clickhouse
        then
            break
        fi
        sleep 1
    done
}

function get_profiles
{
    # Collect the profiles
    clickhouse-client --port 9001 --query "set query_profiler_cpu_time_period_ns = 0"
    clickhouse-client --port 9001 --query "set query_profiler_real_time_period_ns = 0"
    clickhouse-client --port 9001 --query "system flush logs" &

    clickhouse-client --port 9002 --query "set query_profiler_cpu_time_period_ns = 0"
    clickhouse-client --port 9002 --query "set query_profiler_real_time_period_ns = 0"
    clickhouse-client --port 9002 --query "system flush logs" &

    wait

    clickhouse-client --port 9001 --query "select * from system.query_log where type = 2 format TSVWithNamesAndTypes" > left-query-log.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.query_thread_log format TSVWithNamesAndTypes" > left-query-thread-log.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.trace_log format TSVWithNamesAndTypes" > left-trace-log.tsv ||: &
    clickhouse-client --port 9001 --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > left-addresses.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.metric_log format TSVWithNamesAndTypes" > left-metric-log.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.asynchronous_metric_log format TSVWithNamesAndTypes" > left-async-metric-log.tsv ||: &

    clickhouse-client --port 9002 --query "select * from system.query_log where type = 2 format TSVWithNamesAndTypes" > right-query-log.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.query_thread_log format TSVWithNamesAndTypes" > right-query-thread-log.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.trace_log format TSVWithNamesAndTypes" > right-trace-log.tsv ||: &
    clickhouse-client --port 9002 --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > right-addresses.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.metric_log format TSVWithNamesAndTypes" > right-metric-log.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.asynchronous_metric_log format TSVWithNamesAndTypes" > right-async-metric-log.tsv ||: &

    wait

    # Just check that the servers are alive so that we return a proper exit code.
    # We don't consistently check the return codes of the above background jobs.
    clickhouse-client --port 9001 --query "select 1"
    clickhouse-client --port 9002 --query "select 1"
}

function build_log_column_definitions
{
# FIXME This loop builds column definitons from TSVWithNamesAndTypes in an
# absolutely atrocious way. This should be done by the file() function itself.
for x in {right,left}-{addresses,{query,query-thread,trace,{async-,}metric}-log}.tsv
do
    paste -d' ' \
        <(sed -n '1{s/\t/\n/g;p;q}' "$x" | sed 's/\(^.*$\)/"\1"/') \
        <(sed -n '2{s/\t/\n/g;p;q}' "$x" ) \
        | tr '\n' ', ' | sed 's/,$//' > "$x.columns"
done
}

# Build and analyze randomization distribution for all queries.
function analyze_queries
{
rm -v analyze-commands.txt analyze-errors.log all-queries.tsv unstable-queries.tsv ./*-report.tsv raw-queries.tsv ||:
rm -rf analyze ||:
mkdir analyze analyze/tmp ||:

build_log_column_definitions

# Split the raw test output into files suitable for analysis.
IFS=$'\n'
for test_file in $(find . -maxdepth 1 -name "*-raw.tsv" -print)
do
    test_name=$(basename "$test_file" "-raw.tsv")
    sed -n "s/^query\t/$test_name\t/p" < "$test_file" >> "analyze/query-runs.tsv"
    sed -n "s/^client-time\t/$test_name\t/p" < "$test_file" >> "analyze/client-times.tsv"
    sed -n "s/^report-threshold\t/$test_name\t/p" < "$test_file" >> "analyze/report-thresholds.tsv"
    sed -n "s/^skipped\t/$test_name\t/p" < "$test_file" >> "analyze/skipped-tests.tsv"
    sed -n "s/^display-name\t/$test_name\t/p" < "$test_file" >> "analyze/query-display-names.tsv"
    sed -n "s/^short\t/$test_name\t/p" < "$test_file" >> "analyze/marked-short-queries.tsv"
    sed -n "s/^partial\t/$test_name\t/p" < "$test_file" >> "analyze/partial-queries.tsv"
done
unset IFS

# for each query run, prepare array of metrics from query log
clickhouse-local --query "
create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

-- Separately process 'partial' queries which we could only run on the new server
-- because they use new functions. We can't make normal stats for them, but still
-- have to show some stats so that the PR author can tweak them.
create view partial_queries as select test, query_index
    from file('analyze/partial-queries.tsv', TSV,
        'test text, query_index int, servers Array(int)');

create table partial_query_times engine File(TSVWithNamesAndTypes,
        'analyze/partial-query-times.tsv')
    as select test, query_index, stddevPop(time) time_stddev, median(time) time_median
    from query_runs
    where (test, query_index) in partial_queries
    group by test, query_index
    ;

-- Process queries that were run normally, on both servers.
create view left_query_log as select *
    from file('left-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "left-query-log.tsv.columns")');

create view right_query_log as select *
    from file('right-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "right-query-log.tsv.columns")');

create view query_logs as
    select 0 version, query_id, ProfileEvents.Names, ProfileEvents.Values,
        query_duration_ms from left_query_log
    union all
    select 1 version, query_id, ProfileEvents.Names, ProfileEvents.Values,
        query_duration_ms from right_query_log
    ;

-- This is a single source of truth on all metrics we have for query runs. The
-- metrics include ProfileEvents from system.query_log, and query run times
-- reported by the perf.py test runner.
create table query_run_metric_arrays engine File(TSV, 'analyze/query-run-metric-arrays.tsv')
    as
    with (
        -- sumMapState with the list of all keys with '-0.' values. Negative zero is because
        -- sumMap removes keys with positive zeros.
        with (select groupUniqArrayArray(ProfileEvents.Names) from query_logs) as all_names
            select arrayReduce('sumMapState', [(all_names, arrayMap(x->-0., all_names))])
        ) as all_metrics
    select test, query_index, version, query_id,
        (finalizeAggregation(
            arrayReduce('sumMapMergeState',
                [
                    all_metrics,
                    arrayReduce('sumMapState',
                        [(ProfileEvents.Names,
                            arrayMap(x->toFloat64(x), ProfileEvents.Values))]
                    ),
                    arrayReduce('sumMapState', [(
                        ['client_time', 'server_time'],
                        arrayMap(x->if(x != 0., x, -0.), [
                            toFloat64(query_runs.time),
                            toFloat64(query_duration_ms / 1000.)]))])
                ]
            )) as metrics_tuple).1 metric_names,
        metrics_tuple.2 metric_values
    from query_logs
    right join query_runs
        on query_logs.query_id = query_runs.query_id
            and query_logs.version = query_runs.version
    where (test, query_index) not in partial_queries
    ;

-- This is just for convenience -- human-readable + easy to make plots.
create table query_run_metrics_denorm engine File(TSV, 'analyze/query-run-metrics-denorm.tsv')
    as select test, query_index, metric_names, version, query_id, metric_values
    from query_run_metric_arrays
    array join metric_names, metric_values
    order by test, query_index, metric_names, version, query_id
    ;

-- This is for statistical processing with eqmed.sql
create table query_run_metrics_for_stats engine File(
        TSV, -- do not add header -- will parse with grep
        'analyze/query-run-metrics-for-stats.tsv')
    as select test, query_index, 0 run, version, metric_values
    from query_run_metric_arrays
    order by test, query_index, run, version
    ;

-- This is the list of metric names, so that we can join them back after
-- statistical processing.
create table query_run_metric_names engine File(TSV, 'analyze/query-run-metric-names.tsv')
    as select metric_names from query_run_metric_arrays limit 1
    ;
" 2> >(tee -a analyze/errors.log 1>&2)

# This is a lateral join in bash... please forgive me.
# We don't have arrayPermute(), so I have to make random permutations with
# `order by rand`, and it becomes really slow if I do it for more than one
# query. We also don't have lateral joins. So I just put all runs of each
# query into a separate file, and then compute randomization distribution
# for each file. I do this in parallel using GNU parallel.
( set +x # do not bloat the log
IFS=$'\n'
for prefix in $(cut -f1,2 "analyze/query-run-metrics-for-stats.tsv" | sort | uniq)
do
    file="analyze/tmp/$(echo "$prefix" | sed 's/\t/_/g').tsv"
    grep "^$prefix	" "analyze/query-run-metrics-for-stats.tsv" > "$file" &
    printf "%s\0\n" \
        "clickhouse-local \
            --file \"$file\" \
            --structure 'test text, query text, run int, version UInt8, metrics Array(float)' \
            --query \"$(cat "$script_dir/eqmed.sql")\" \
            >> \"analyze/query-metric-stats.tsv\"" \
            2>> analyze/errors.log \
        >> analyze/commands.txt
done
wait
unset IFS
)

parallel --joblog analyze/parallel-log.txt --null < analyze/commands.txt 2>> analyze/errors.log

clickhouse-local --query "
-- Join the metric names back to the metric statistics we've calculated, and make
-- a denormalized table of them -- statistics for all metrics for all queries.
-- The WITH, ARRAY JOIN and CROSS JOIN do not like each other:
--  https://github.com/ClickHouse/ClickHouse/issues/11868
--  https://github.com/ClickHouse/ClickHouse/issues/11757
-- Because of this, we make a view with arrays first, and then apply all the
-- array joins.
create view query_metric_stat_arrays as
    with (select * from file('analyze/query-run-metric-names.tsv',
        TSV, 'n Array(String)')) as metric_name
    select test, query_index, metric_name, left, right, diff, stat_threshold
    from file('analyze/query-metric-stats.tsv', TSV, 'left Array(float),
        right Array(float), diff Array(float), stat_threshold Array(float),
        test text, query_index int') reports
    order by test, query_index, metric_name
    ;

create table query_metric_stats_denorm engine File(TSVWithNamesAndTypes,
        'analyze/query-metric-stats-denorm.tsv')
    as select test, query_index, metric_name, left, right, diff, stat_threshold
    from query_metric_stat_arrays
    left array join metric_name, left, right, diff, stat_threshold
    order by test, query_index, metric_name
    ;
" 2> >(tee -a analyze/errors.log 1>&2)
}

# Analyze results
function report
{
rm -r report ||:
mkdir report report/tmp ||:

rm ./*.{rep,svg} test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv run-errors.tsv ||:

build_log_column_definitions

cat analyze/errors.log >> report/errors.log ||:
cat profile-errors.log >> report/errors.log ||:

short_query_threshold="0.02"

clickhouse-local --query "
create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create view partial_query_times as select * from
    file('analyze/partial-query-times.tsv', TSVWithNamesAndTypes,
        'test text, query_index int, time_stddev float, time_median float')
    ;

-- Report for partial queries that we could only run on the new server (e.g.
-- queries with new functions added in the tested PR).
create table partial_queries_report engine File(TSV, 'report/partial-queries-report.tsv')
    as select toDecimal64(time_median, 3) time,
        toDecimal64(time_stddev / time_median, 3) relative_time_stddev,
        test, query_index, query_display_name
    from partial_query_times
    join query_display_names using (test, query_index)
    order by test, query_index
    ;

create view query_metric_stats as
    select * from file('analyze/query-metric-stats-denorm.tsv',
        TSVWithNamesAndTypes,
        'test text, query_index int, metric_name text, left float, right float,
            diff float, stat_threshold float')
    ;

-- Main statistics for queries -- query time as reported in query log.
create table queries engine File(TSVWithNamesAndTypes, 'report/queries.tsv')
    as select
        -- Comparison mode doesn't make sense for queries that complete
        -- immediately (on the same order of time as noise). If query duration is
        -- less that some threshold, we just skip it. If there is a significant
        -- regression in such query, the time will exceed the threshold, and we
        -- well process it normally and detect the regression.
        right < $short_query_threshold as short,

        not short and abs(diff) > report_threshold        and abs(diff) > stat_threshold as changed_fail,
        not short and abs(diff) > report_threshold - 0.05 and abs(diff) > stat_threshold as changed_show,
        
        not short and not changed_fail and stat_threshold > report_threshold + 0.10 as unstable_fail,
        not short and not changed_show and stat_threshold > report_threshold - 0.05 as unstable_show,
        
        left, right, diff, stat_threshold,
        if(report_threshold > 0, report_threshold, 0.10) as report_threshold,
        query_metric_stats.test test, query_metric_stats.query_index query_index,
        query_display_name
    from query_metric_stats
    left join file('analyze/report-thresholds.tsv', TSV,
            'test text, report_threshold float') thresholds
        on query_metric_stats.test = thresholds.test
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    where metric_name = 'server_time'
    order by test, query_index, metric_name
    ;

create table changed_perf_report engine File(TSV, 'report/changed-perf.tsv') as
    select
        toDecimal64(left, 3), toDecimal64(right, 3),
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        left > right
            ? '- ' || toString(toDecimal64(left / (right + 0.001), 3)) || 'x'
            : '+ ' || toString(toDecimal64(right / (left + 0.001), 3)) || 'x',
         toDecimal64(diff, 3), toDecimal64(stat_threshold, 3),
         changed_fail, test, query_index, query_display_name
    from queries where changed_show order by abs(diff) desc;

create table unstable_queries_report engine File(TSV, 'report/unstable-queries.tsv') as
    select
        toDecimal64(left, 3), toDecimal64(right, 3), toDecimal64(diff, 3),
        toDecimal64(stat_threshold, 3), unstable_fail, test, query_index, query_display_name
    from queries where unstable_show order by stat_threshold desc;

create table test_time_changes engine File(TSV, 'report/test-time-changes.tsv') as
    select test, queries, average_time_change from (
        select test, count(*) queries,
            sum(left) as left, sum(right) as right,
            (right - left) / right average_time_change
        from queries
        group by test
        order by abs(average_time_change) desc
    )
    ;

create table unstable_tests engine File(TSV, 'report/unstable-tests.tsv') as
    select test, sum(unstable_show) total_unstable, sum(changed_show) total_changed
    from queries
    group by test
    order by total_unstable + total_changed desc
    ;

create table test_perf_changes_report engine File(TSV, 'report/test-perf-changes.tsv') as
    select test,
        queries,
        coalesce(total_unstable, 0) total_unstable,
        coalesce(total_changed, 0) total_changed,
        total_unstable + total_changed total_bad,
        coalesce(toString(toDecimal64(average_time_change, 3)), '??') average_time_change_str
    from test_time_changes
    full join unstable_tests
    using test
    where (abs(average_time_change) > 0.05 and queries > 5)
        or (total_bad > 0)
    order by total_bad desc, average_time_change desc
    settings join_use_nulls = 1
    ;

create view total_client_time_per_query as select *
    from file('analyze/client-times.tsv', TSV,
        'test text, query_index int, client float, server float');

create table slow_on_client_report engine File(TSV, 'report/slow-on-client.tsv') as
    select client, server, toDecimal64(client/server, 3) p,
        test, query_display_name
    from total_client_time_per_query left join query_display_names using (test, query_index)
    where p > toDecimal64(1.02, 3) order by p desc;

create table wall_clock_time_per_test engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

create table test_time engine Memory as
    select test, sum(client) total_client_time,
        maxIf(client, not short) query_max,
        minIf(client, not short) query_min,
        count(*) queries, sum(short) short_queries
    from total_client_time_per_query full join queries using (test, query_index)
    group by test;

create table test_times_report engine File(TSV, 'report/test-times.tsv') as
    select wall_clock_time_per_test.test, real,
        toDecimal64(total_client_time, 3),
        queries,
        short_queries,
        toDecimal64(query_max, 3),
        toDecimal64(real / queries, 3) avg_real_per_query,
        toDecimal64(query_min, 3)
    from test_time
    -- wall clock times are also measured for skipped tests, so don't
    -- do full join
    left join wall_clock_time_per_test using test
    order by avg_real_per_query desc;

-- report for all queries page, only main metric
create table all_tests_report engine File(TSV, 'report/all-queries.tsv') as
    select changed_fail, unstable_fail,
        toDecimal64(left, 3), toDecimal64(right, 3),
        left > right
            ? '- ' || toString(toDecimal64(left / (right + 0.001), 3)) || 'x'
            : '+ ' || toString(toDecimal64(right / (left + 0.001), 3)) || 'x',
        toDecimal64(isFinite(diff) ? diff : 0, 3),
        toDecimal64(isFinite(stat_threshold) ? stat_threshold : 0, 3),
        test, query_index, query_display_name
    from queries order by test, query_index;

-- queries for which we will build flamegraphs (see below)
create table queries_for_flamegraph engine File(TSVWithNamesAndTypes,
        'report/queries-for-flamegraph.tsv') as
    select test, query_index from queries where unstable_show or changed_show
    ;

-- List of queries that have 'short' duration, but are not marked as 'short' by
-- the test author (we report them).
create table unmarked_short_queries_report
    engine File(TSV, 'report/unmarked-short-queries.tsv')
    as select time, test, query_index, query_display_name
    from (
            select right time, test, query_index from queries where short
            union all
            select time_median, test, query_index from partial_query_times
                where time_median < $short_query_threshold
        ) times
        left join query_display_names
            on times.test = query_display_names.test
                and times.query_index = query_display_names.query_index
    where (test, query_index) not in
        (select * from file('analyze/marked-short-queries.tsv', TSV,
            'test text, query_index int'))
    order by test, query_index
    ;

--------------------------------------------------------------------------------
-- various compatibility data formats follow, not related to the main report

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select short, changed_fail, unstable_fail, left, right, diff,
        stat_threshold, test, query_display_name query
    from queries
    ;

-- new report for all queries with all metrics (no page yet)
create table all_query_metrics_tsv engine File(TSV, 'report/all-query-metrics.tsv') as
    select metric_name, left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold, test, query_index, query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    order by test, query_index;
" 2> >(tee -a report/errors.log 1>&2)


# Prepare source data for metrics and flamegraphs for unstable queries.
for version in {right,left}
do
    rm -rf data
    clickhouse-local --query "
create view queries_for_flamegraph as
    select * from file('report/queries-for-flamegraph.tsv', TSVWithNamesAndTypes,
        'test text, query_index int');

create view query_runs as
    with 0 as left, 1 as right
    select * from file('analyze/query-runs.tsv', TSV,
        'test text, query_index int, query_id text, version UInt8, time float')
    where version = $version
    ;

create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create table unstable_query_runs engine File(TSVWithNamesAndTypes,
        'unstable-query-runs.$version.rep') as
    select query_runs.test test, query_runs.query_index query_index,
        query_display_name, query_id
    from query_runs
    join queries_for_flamegraph on
        query_runs.test = queries_for_flamegraph.test
        and query_runs.query_index = queries_for_flamegraph.query_index
    left join query_display_names on
        query_runs.test = query_display_names.test
        and query_runs.query_index = query_display_names.query_index
    ;

create view query_log as select *
    from file('$version-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-query-log.tsv.columns")');

create table unstable_run_metrics engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics.$version.rep') as
    select
        test, query_index, query_id,
        ProfileEvents.Values value, ProfileEvents.Names metric
    from query_log array join ProfileEvents
    join unstable_query_runs using (query_id)
    ;

create table unstable_run_metrics_2 engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics-2.$version.rep') as
    select
        test, query_index, query_id,
        v, n
    from (
        select
            test, query_index, query_id,
            ['memory_usage', 'read_bytes', 'written_bytes', 'query_duration_ms'] n,
            [memory_usage, read_bytes, written_bytes, query_duration_ms] v
        from query_log
        join unstable_query_runs using (query_id)
    )
    array join v, n;

create view trace_log as select *
    from file('$version-trace-log.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-trace-log.tsv.columns")');

create view addresses_src as select addr,
        -- Some functions change name between builds, e.g. '__clone' or 'clone' or
        -- even '__GI__clone@@GLIBC_2.32'. This breaks differential flame graphs, so
        -- filter them out here.
        [name, 'clone.S (filtered by script)', 'pthread_cond_timedwait (filtered by script)']
            -- this line is a subscript operator of the above array
            [1 + multiSearchFirstIndex(name, ['clone.S', 'pthread_cond_timedwait'])] name
    from file('$version-addresses.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-addresses.tsv.columns")');

create table addresses_join_$version engine Join(any, left, address) as
    select addr address, name from addresses_src;

create table unstable_run_traces engine File(TSVWithNamesAndTypes,
        'unstable-run-traces.$version.rep') as
    select
        test, query_index, query_id,
        count() value,
        joinGet(addresses_join_$version, 'name', arrayJoin(trace))
            || '(' || toString(trace_type) || ')' metric
    from trace_log
    join unstable_query_runs using query_id
    group by test, query_index, query_id, metric
    order by count() desc
    ;

create table metric_devation engine File(TSVWithNamesAndTypes,
        'report/metric-deviation.$version.tsv') as
    -- first goes the key used to split the file with grep
    select test, query_index, query_display_name,
        toDecimal64(d, 3) d, q, metric
    from (
        select
            test, query_index,
            (q[3] - q[1])/q[2] d,
            quantilesExact(0, 0.5, 1)(value) q, metric
        from (select * from unstable_run_metrics
            union all select * from unstable_run_traces
            union all select * from unstable_run_metrics_2) mm
        group by test, query_index, metric
        having isFinite(d) and d > 0.5 and q[3] > 5
    ) metrics
    left join query_display_names using (test, query_index)
    order by test, query_index, d desc
    ;

create table stacks engine File(TSV, 'report/stacks.$version.tsv') as
    select
        -- first goes the key used to split the file with grep
        test, query_index, trace_type, any(query_display_name),
        -- next go the stacks in flamegraph format: 'func1;...;funcN count'
        arrayStringConcat(
            arrayMap(
                addr -> joinGet(addresses_join_$version, 'name', addr),
                arrayReverse(trace)
            ),
            ';'
        ) readable_trace,
        count() c
    from trace_log
    join unstable_query_runs using query_id
    group by test, query_index, trace_type, trace
    order by test, query_index, trace_type, trace
    ;
" 2> >(tee -a report/errors.log 1>&2) &
done
wait

# Create per-query flamegraphs
IFS=$'\n'
for version in {right,left}
do
    for query in $(cut -d'	' -f1-4 "report/stacks.$version.tsv" | sort | uniq)
    do
        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')
        echo "$query_file" >> report/query-files.txt

        # Build separate .svg flamegraph for each query.
        # -F is somewhat unsafe because it might match not the beginning of the
        # string, but this is unlikely and escaping the query for grep is a pain.
        grep -F "$query	" "report/stacks.$version.tsv" \
            | cut -f 5- \
            | sed 's/\t/ /g' \
            | tee "report/tmp/$query_file.stacks.$version.tsv" \
            | ~/fg/flamegraph.pl --hash > "$query_file.$version.svg" &
    done
done
wait
unset IFS

# Create differential flamegraphs.
IFS=$'\n'
for query_file in $(cat report/query-files.txt)
do
    ~/fg/difffolded.pl "report/tmp/$query_file.stacks.left.tsv" \
            "report/tmp/$query_file.stacks.right.tsv" \
        | tee "report/tmp/$query_file.stacks.diff.tsv" \
        | ~/fg/flamegraph.pl > "$query_file.diff.svg" &
done
unset IFS
wait

# Create per-query files with metrics. Note that the key is different from flamegraphs.
IFS=$'\n'
for version in {right,left}
do
    for query in $(cut -d'	' -f1-3 "report/metric-deviation.$version.tsv" | sort | uniq)
    do
        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')

        # Ditto the above comment about -F.
        grep -F "$query	" "report/metric-deviation.$version.tsv" \
            | cut -f4- > "$query_file.$version.metrics.rep" &
    done
done
wait
unset IFS

# Prefer to grep for clickhouse_driver exception messages, but if there are none,
# just show a couple of lines from the log.
for log in *-err.log
do
    test=$(basename "$log" "-err.log")
    {
        grep -H -m2 -i '\(Exception\|Error\):[^:]' "$log" \
            || head -2 "$log"
    } | sed "s/^/$test\t/" >> run-errors.tsv ||:
done
}

function report_metrics
{
rm -rf metrics ||:
mkdir metrics

clickhouse-local --query "
create view right_async_metric_log as
    select * from file('right-async-metric-log.tsv', TSVWithNamesAndTypes,
        'event_date Date, event_time DateTime, name String, value Float64')
    ;

-- Use the right log as time reference because it may have higher precision.
create table metrics engine File(TSV, 'metrics/metrics.tsv') as
    with (select min(event_time) from right_async_metric_log) as min_time
    select name metric, r.event_time - min_time event_time, l.value as left, r.value as right
    from right_async_metric_log r
    asof join file('left-async-metric-log.tsv', TSVWithNamesAndTypes,
        'event_date Date, event_time DateTime, name String, value Float64') l
    on l.name = r.name and r.event_time <= l.event_time
    order by metric, event_time
    ;

-- Show metrics that have changed
create table changes engine File(TSV, 'metrics/changes.tsv') as
    select metric, left, right,
        toDecimal64(diff, 3), toDecimal64(times_diff, 3)
    from (
        select metric, median(left) as left, median(right) as right,
            (right - left) / left diff,
            if(left > right, left / right, right / left) times_diff
        from metrics
        group by metric
        having abs(diff) > 0.05 and isFinite(diff)
    )
    order by diff desc
    ;
"
2> >(tee -a metrics/errors.log 1>&2)

IFS=$'\n'
for prefix in $(cut -f1 "metrics/metrics.tsv" | sort | uniq)
do
    file="metrics/$prefix.tsv"
    grep "^$prefix	" "metrics/metrics.tsv" | cut -f2- > "$file"

    gnuplot -e "
        set datafile separator '\t';
        set terminal png size 960,540;
        set xtics time format '%tH:%tM';
        set title '$prefix' noenhanced offset 0,-3;
        set key left top;
        plot
            '$file' using 1:2 with lines title 'Left'
            , '$file' using 1:3 with lines title 'Right'
            ;
    " \
        | convert - -filter point -resize "200%" "metrics/$prefix.png" &

done
wait
unset IFS
}

# Check that local and client are in PATH
clickhouse-local --version > /dev/null
clickhouse-client --version > /dev/null

case "$stage" in
"")
    ;&
"configure")
    time configure
    ;&
"restart")
    numactl --hardware ||:
    lscpu ||:
    time restart
    ;&
"run_tests")
    # Ignore the errors to collect the log and build at least some report, anyway
    time run_tests ||:
    ;&
"run_benchmark")
    time run_benchmark 2> >(tee -a run-errors.tsv 1>&2) ||:
    ;&
"get_profiles")
    # Check for huge pages.
    cat /sys/kernel/mm/transparent_hugepage/enabled > thp-enabled.txt ||:
    cat /proc/meminfo > meminfo.txt ||:
    for pid in $(pgrep -f clickhouse-server)
    do
        cat "/proc/$pid/smaps" > "$pid-smaps.txt" ||:
    done

    # We had a bug where getting profiles froze sometimes, so try to save some
    # logs if this happens again. Give the servers some time to collect all info,
    # then trace and kill. Start in a subshell, so that both function don't
    # interfere with each other's jobs through `wait`. Also make the subshell
    # have its own process group, so that we can then kill it with all its child
    # processes. Somehow it doesn't kill the children by itself when dying.
    set -m
    ( get_profiles_watchdog ) &
    watchdog_pid=$!
    set +m
    # Check that the watchdog started OK.
    kill -0 $watchdog_pid

    # If the tests fail with OOM or something, still try to restart the servers
    # to collect the logs. Prefer not to restart, because addresses might change
    # and we won't be able to process trace_log data. Start in a subshell, so that
    # it doesn't interfere with the watchdog through `wait`.
    ( get_profiles || restart && get_profiles ||: )

    # Kill the whole process group, because somehow when the subshell is killed,
    # the sleep inside remains alive and orphaned.
    while env kill -- -$watchdog_pid ; do sleep 1; done

    # Stop the servers to free memory for the subsequent query analysis.
    while killall clickhouse; do echo . ; sleep 1 ; done
    echo Servers stopped.
    ;&
"analyze_queries")
    time analyze_queries ||:
    ;&
"report")
    time report ||:
    ;&
"report_metrics")
    time report_metrics ||:
    cat metrics/errors.log >> report/errors.log ||:
    ;&
"report_html")
    time "$script_dir/report.py" --report=all-queries > all-queries.html 2> >(tee -a report/errors.log 1>&2) ||:
    time "$script_dir/report.py" > report.html
    ;&
esac

# Print some final debug info to help debug Weirdness, of which there is plenty.
jobs
pstree -apgT
