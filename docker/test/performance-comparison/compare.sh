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
    left/clickhouse-server --config-file=left/config/config.xml -- --path db0 &> setup-server-log.log &
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

    # Remove logs etc, because they will be updated, and sharing them between
    # servers with hardlink might cause unpredictable behavior.
    rm db0/data/system/* -rf ||:
    rm db0/metadata/system/* -rf ||:

    # Make copies of the original db for both servers. Use hardlinks instead
    # of copying. Be careful to remove preprocessed configs and system tables,or
    # it can lead to weird effects.
    rm -r left/db ||:
    rm -r right/db ||:
    rm -r db0/preprocessed_configs ||:
    rm -r db/{data,metadata}/system ||:
    cp -al db0/ left/db/
    cp -al db0/ right/db/
}

function restart
{
    while killall clickhouse-server; do echo . ; sleep 1 ; done
    echo all killed

    set -m # Spawn servers in their own process groups

    left/clickhouse-server --config-file=left/config/config.xml -- --path left/db &>> left-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid

    right/clickhouse-server --config-file=right/config/config.xml -- --path right/db &>> right-server-log.log &
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
    elif [ "$PR_TO_TEST" != "" ] && [ "$PR_TO_TEST" != "0" ]
    then
        # For PRs, use newer test files so we can test these changes.
        test_prefix=right/performance

        # If some tests were changed in the PR, we may want to run only these
        # ones. The list of changed tests in changed-test.txt is prepared in
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

    # Delete old report files.
    for x in {test-times,wall-clock-times}.tsv
    do
        rm -v "$x" ||:
        touch "$x"
    done

    # Run the tests.
    test_name="<none>"
    for test in $test_files
    do
        # Check that both servers are alive, to fail faster if they die.
        clickhouse-client --port 9001 --query "select 1 format Null" \
            || { echo $test_name >> left-server-died.log ; restart ; continue ; }
        clickhouse-client --port 9002 --query "select 1 format Null" \
            || { echo $test_name >> right-server-died.log ; restart ; continue ; }

        test_name=$(basename "$test" ".xml")
        echo test "$test_name"

        TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
        # the grep is to filter out set -x output and keep only time output
        { time "$script_dir/perf.py" --host localhost localhost --port 9001 9002 -- "$test" > "$test_name-raw.tsv" 2> "$test_name-err.log" ; } 2>&1 >/dev/null | grep -v ^+ >> "wall-clock-times.tsv" || continue
    done

    unset TIMEFORMAT

    wait
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
    clickhouse-client --port 9001 --query "set query_profiler_cpu_time_period_ns = 0"
    clickhouse-client --port 9001 --query "set query_profiler_real_time_period_ns = 0"
    clickhouse-client --port 9001 --query "system flush logs"
    clickhouse-client --port 9002 --query "system flush logs"

    clickhouse-client --port 9001 --query "select * from system.query_log where type = 2 format TSVWithNamesAndTypes" > left-query-log.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.query_thread_log format TSVWithNamesAndTypes" > left-query-thread-log.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.trace_log format TSVWithNamesAndTypes" > left-trace-log.tsv ||: &
    clickhouse-client --port 9001 --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > left-addresses.tsv ||: &
    clickhouse-client --port 9001 --query "select * from system.metric_log format TSVWithNamesAndTypes" > left-metric-log.tsv ||: &

    clickhouse-client --port 9002 --query "select * from system.query_log where type = 2 format TSVWithNamesAndTypes" > right-query-log.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.query_thread_log format TSVWithNamesAndTypes" > right-query-thread-log.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.trace_log format TSVWithNamesAndTypes" > right-trace-log.tsv ||: &
    clickhouse-client --port 9002 --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > right-addresses.tsv ||: &
    clickhouse-client --port 9002 --query "select * from system.metric_log format TSVWithNamesAndTypes" > right-metric-log.tsv ||: &

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
for x in {right,left}-{addresses,{query,query-thread,trace,metric}-log}.tsv
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
    sed -n "s/^client-time/$test_name/p" < "$test_file" >> "analyze/client-times.tsv"
    sed -n "s/^report-threshold/$test_name/p" < "$test_file" >> "analyze/report-thresholds.tsv"
    sed -n "s/^skipped/$test_name/p" < "$test_file" >> "analyze/skipped-tests.tsv"
    sed -n "s/^display-name/$test_name/p" < "$test_file" >> "analyze/query-display-names.tsv"
done
unset IFS

# for each query run, prepare array of metrics from query log
clickhouse-local --query "
create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

create view left_query_log as select *
    from file('left-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "left-query-log.tsv.columns")');

create view right_query_log as select *
    from file('right-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "right-query-log.tsv.columns")');

create table query_metrics engine File(TSV, -- do not add header -- will parse with grep
        'analyze/query-run-metrics.tsv')
    as select
        test, query_index, 0 run, version,
        [
            -- server-reported time
            query_duration_ms / toFloat64(1000)
            , toFloat64(memory_usage)
            -- client-reported time
            , query_runs.time
        ] metrics
    from (
        select query_duration_ms, memory_usage, query_id, 0 version from left_query_log
        union all
        select query_duration_ms, memory_usage, query_id, 1 version from right_query_log
    ) query_logs
    right join query_runs
    using (query_id, version)
    order by test, query_index
    ;
"

# This is a lateral join in bash... please forgive me.
# We don't have arrayPermute(), so I have to make random permutations with
# `order by rand`, and it becomes really slow if I do it for more than one
# query. We also don't have lateral joins. So I just put all runs of each
# query into a separate file, and then compute randomization distribution
# for each file. I do this in parallel using GNU parallel.
query_index=1
IFS=$'\n'
for prefix in $(cut -f1,2 "analyze/query-run-metrics.tsv" | sort | uniq)
do
    file="analyze/tmp/$(echo "$prefix" | sed 's/\t/_/g').tsv"
    grep "^$prefix	" "analyze/query-run-metrics.tsv" > "$file" &
    printf "%s\0\n" \
        "clickhouse-local \
            --file \"$file\" \
            --structure 'test text, query text, run int, version UInt8, metrics Array(float)' \
            --query \"$(cat "$script_dir/eqmed.sql")\" \
            >> \"analyze/query-reports.tsv\"" \
            2>> analyze/errors.log \
        >> analyze/commands.txt
done
wait
unset IFS

parallel --joblog analyze/parallel-log.txt --null < analyze/commands.txt 2>> analyze/errors.log
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

clickhouse-local --query "
create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create table query_metric_stats engine File(TSVWithNamesAndTypes,
        'report/query-metric-stats.tsv') as
    select metric_name, left, right, diff, stat_threshold, test, query_index,
        query_display_name
    from file ('analyze/query-reports.tsv', TSV, 'left Array(float),
        right Array(float), diff Array(float), stat_threshold Array(float),
        test text, query_index int') reports
    left array join ['server_time', 'memory', 'client_time'] as metric_name,
        left, right, diff, stat_threshold
    left join query_display_names
        on reports.test = query_display_names.test
            and reports.query_index = query_display_names.query_index
    ;

-- Main statistics for queries -- query time as reported in query log.
create table queries engine File(TSVWithNamesAndTypes, 'report/queries.tsv')
    as select
        -- FIXME Comparison mode doesn't make sense for queries that complete
        -- immediately, so for now we pretend they don't exist. We don't want to
        -- remove them altogether because we want to be able to detect regressions,
        -- but the right way to do this is not yet clear.
        (left + right) / 2 < 0.02 as short,

        not short and abs(diff) > report_threshold        and abs(diff) > stat_threshold as changed_fail,
        not short and abs(diff) > report_threshold - 0.05 and abs(diff) > stat_threshold as changed_show,
        
        not short and not changed_fail and stat_threshold > report_threshold + 0.10 as unstable_fail,
        not short and not changed_show and stat_threshold > report_threshold - 0.05 as unstable_show,
        
        left, right, diff, stat_threshold,
        if(report_threshold > 0, report_threshold, 0.10) as report_threshold,
        test, query_index, query_display_name
    from query_metric_stats
    left join file('analyze/report-thresholds.tsv', TSV,
            'test text, report_threshold float') thresholds
        on query_metric_stats.test = thresholds.test
    where metric_name = 'server_time'
    order by test, query_index, metric_name
    ;

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select short, changed_fail, unstable_fail, left, right, diff,
        stat_threshold, test, query_display_name query
    from queries
    ;

-- save all test runs as JSON for the new comparison page
create table all_query_runs_json engine File(JSON, 'report/all-query-runs.json') as
    select test, query_index, query_display_name query,
        left, right, diff, stat_threshold, report_threshold,
        versions_runs[1] runs_left, versions_runs[2] runs_right
    from (
        select
            test, query_index,
            groupArrayInsertAt(runs, version) versions_runs
        from (
            select
                test, query_index, version,
                groupArray(metrics[1]) runs
            from file('analyze/query-run-metrics.tsv', TSV,
                'test text, query_index int, run int, version UInt8, metrics Array(float)')
            group by test, query_index, version
        )
        group by test, query_index
    ) runs
    left join query_display_names
        on runs.test = query_display_names.test
            and runs.query_index = query_display_names.query_index
    left join file('analyze/report-thresholds.tsv',
            TSV, 'test text, report_threshold float') thresholds
        on runs.test = thresholds.test
    left join query_metric_stats
        on runs.test = query_metric_stats.test
            and runs.query_index = query_metric_stats.query_index
    where
        query_metric_stats.metric_name = 'server_time'
    ;

create table changed_perf_tsv engine File(TSV, 'report/changed-perf.tsv') as
    select left, right, diff, stat_threshold, changed_fail, test, query_display_name
    from queries where changed_show order by abs(diff) desc;

create table unstable_queries_tsv engine File(TSV, 'report/unstable-queries.tsv') as
    select left, right, diff, stat_threshold, unstable_fail, test, query_display_name
    from queries where unstable_show order by stat_threshold desc;

create table queries_for_flamegraph engine File(TSVWithNamesAndTypes,
        'report/queries-for-flamegraph.tsv') as
    select test, query_index from queries where unstable_show or changed_show
    ;

create table unstable_tests_tsv engine File(TSV, 'report/bad-tests.tsv') as
    select test, sum(unstable_fail) u, sum(changed_fail) c, u + c s from queries
    group by test having s > 0 order by s desc;

create table query_time engine Memory as select *
    from file('analyze/client-times.tsv', TSV,
        'test text, query_index int, client float, server float');

create table wall_clock engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

create table slow_on_client_tsv engine File(TSV, 'report/slow-on-client.tsv') as
    select client, server, floor(client/server, 3) p, test, query_display_name
    from query_time left join query_display_names using (test, query_index)
    where p > 1.02 order by p desc;

create table test_time engine Memory as
    select test, sum(client) total_client_time,
        maxIf(client, not short) query_max,
        minIf(client, not short) query_min,
        count(*) queries, sum(short) short_queries
    from query_time full join queries using (test, query_index)
    group by test;

create table test_times_tsv engine File(TSV, 'report/test-times.tsv') as
    select wall_clock.test, real,
        floor(total_client_time, 3),
        queries,
        short_queries,
        floor(query_max, 3),
        floor(real / queries, 3) avg_real_per_query,
        floor(query_min, 3)
    from test_time
    -- wall clock times are also measured for skipped tests, so don't
    -- do full join
    left join wall_clock using test
    order by avg_real_per_query desc;

-- report for all queries page, only main metric
create table all_tests_tsv engine File(TSV, 'report/all-queries.tsv') as
    select changed_fail, unstable_fail,
        left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold, test, query_display_name
    from queries order by test, query_display_name;

-- new report for all queries with all metrics (no page yet)
create table all_query_metrics_tsv engine File(TSV, 'report/all-query-metrics.tsv') as
    select metric_name, left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold, test, query_index, query_display_name
    from query_metric_stats
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
    select test, query_index, query_display_name, query_id
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
        d, q, metric
    from (
        select
            test, query_index,
            floor((q[3] - q[1])/q[2], 3) d,
            quantilesExact(0, 0.5, 1)(value) q, metric
        from (select * from unstable_run_metrics
            union all select * from unstable_run_traces
            union all select * from unstable_run_metrics_2) mm
        group by test, query_index, metric
        having d > 0.5
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
" 2> >(tee -a report/errors.log 1>&2) # do not run in parallel because they use the same data dir for StorageJoins which leads to weird errors.
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

# Remember that grep sets error code when nothing is found, hence the bayan
# operator.
grep -H -m2 -i '\(Exception\|Error\):[^:]' ./*-err.log | sed 's/:/\t/' >> run-errors.tsv ||:
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
    time restart
    ;&
"run_tests")
    # Ignore the errors to collect the log and build at least some report, anyway
    time run_tests ||:
    ;&
"get_profiles")
    # Getting profiles inexplicably hangs sometimes, so try to save some logs if
    # this happens again. Give the servers some time to collect all info, then
    # trace and kill. Start in a subshell, so that both function don't interfere
    # with each other's jobs through `wait`. Also make the subshell have its own
    # process group, so that we can then kill it with all its child processes.
    # Somehow it doesn't kill the children by itself when dying.
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
    ( get_profiles || restart || get_profiles ||: )

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

    time "$script_dir/report.py" --report=all-queries > all-queries.html 2> >(tee -a report/errors.log 1>&2) ||:
    time "$script_dir/report.py" > report.html
    ;&
esac

# Print some final debug info to help debug Weirdness, of which there is plenty.
jobs
pstree -apgT
