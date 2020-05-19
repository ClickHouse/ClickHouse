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
    while ! clickhouse-client --port 9001 --query "select 1" ; do kill -0 $left_pid ; echo . ; sleep 1 ; done
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

    while ! clickhouse-client --port 9001 --query "select 1" ; do kill -0 $left_pid ; echo . ; sleep 1 ; done
    echo left ok
    while ! clickhouse-client --port 9002 --query "select 1" ; do kill -0 $right_pid ; echo . ; sleep 1 ; done
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
    for x in {test-times,skipped-tests,wall-clock-times,report-thresholds,client-times}.tsv
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

        # The test completed with zero status, so we treat stderr as warnings
        mv "$test_name-err.log" "$test_name-warn.log"
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

# Build and analyze randomization distribution for all queries.
function analyze_queries
{
rm -v analyze-commands.txt analyze-errors.log all-queries.tsv unstable-queries.tsv ./*-report.tsv raw-queries.tsv client-times.tsv report-thresholds.tsv ||:

# Split the raw test output into files suitable for analysis.
IFS=$'\n'
for test_file in $(find . -maxdepth 1 -name "*-raw.tsv" -print)
do
    test_name=$(basename "$test_file" "-raw.tsv")
    sed -n "s/^query\t//p" < "$test_file" > "$test_name-queries.tsv"
    sed -n "s/^client-time/$test_name/p" < "$test_file" >> "client-times.tsv"
    sed -n "s/^report-threshold/$test_name/p" < "$test_file" >> "report-thresholds.tsv"
    sed -n "s/^skipped/$test_name/p" < "$test_file" >> "skipped-tests.tsv"
done
unset IFS

# This is a lateral join in bash... please forgive me.
# We don't have arrayPermute(), so I have to make random permutations with 
# `order by rand`, and it becomes really slow if I do it for more than one
# query. We also don't have lateral joins. So I just put all runs of each
# query into a separate file, and then compute randomization distribution
# for each file. I do this in parallel using GNU parallel.
IFS=$'\n'
for test_file in $(find . -maxdepth 1 -name "*-queries.tsv" -print)
do
    test_name=$(basename "$test_file" "-queries.tsv")
    query_index=1
    for query in $(cut -d'	' -f1 "$test_file" | sort | uniq)
    do
        query_prefix="$test_name.q$query_index"
        query_index=$((query_index + 1))
        grep -F "$query	" "$test_file" > "$query_prefix.tmp"
        printf "%s\0\n" \
            "clickhouse-local \
                --file \"$query_prefix.tmp\" \
                --structure 'query text, run int, version UInt32, time float' \
                --query \"$(cat "$script_dir/eqmed.sql")\" \
                >> \"$test_name-report.tsv\"" \
                2>> analyze-errors.log \
            >> analyze-commands.txt
    done
done
wait
unset IFS

parallel --verbose --null < analyze-commands.txt
}

# Analyze results
function report
{

rm -r report ||:
mkdir report ||:


rm ./*.{rep,svg} test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv ||:

cat analyze-errors.log >> report/errors.log ||:
cat profile-errors.log >> report/errors.log ||:

clickhouse-local --query "
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
        reports.test,
        query
    from
        (
            select *,
                replaceAll(_file, '-report.tsv', '') test
            from file('*-report.tsv', TSV, 'left float, right float, diff float, stat_threshold float, query text')
        ) reports
        left join file('report-thresholds.tsv', TSV, 'test text, report_threshold float') thresholds
        using test
        ;

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select short, changed_fail, unstable_fail, left, right, diff, stat_threshold, test, query
    from queries
    ;

-- save all test runs as JSON for the new comparison page
create table all_query_funs_json engine File(JSON, 'report/all-query-runs.json') as
    select test, query, versions_runs[1] runs_left, versions_runs[2] runs_right
    from (
        select
            test, query,
            groupArrayInsertAt(runs, version) versions_runs
        from (
            select
                replaceAll(_file, '-queries.tsv', '') test,
                query, version,
                groupArray(time) runs
            from file('*-queries.tsv', TSV, 'query text, run int, version UInt32, time float')
            group by test, query, version
        )
        group by test, query
    )
    ;

create table changed_perf_tsv engine File(TSV, 'report/changed-perf.tsv') as
    select left, right, diff, stat_threshold, changed_fail, test, query from queries where changed_show
    order by abs(diff) desc;

create table unstable_queries_tsv engine File(TSV, 'report/unstable-queries.tsv') as
    select left, right, diff, stat_threshold, unstable_fail, test, query from queries where unstable_show
    order by stat_threshold desc;

create table queries_for_flamegraph engine File(TSVWithNamesAndTypes, 'report/queries-for-flamegraph.tsv') as
    select query, test from queries where unstable_show or changed_show
    ;

create table unstable_tests_tsv engine File(TSV, 'report/bad-tests.tsv') as
    select test, sum(unstable_fail) u, sum(changed_fail) c, u + c s from queries
    group by test having s > 0 order by s desc;

create table query_time engine Memory as select *
    from file('client-times.tsv', TSV, 'test text, query text, client float, server float');

create table wall_clock engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

create table slow_on_client_tsv engine File(TSV, 'report/slow-on-client.tsv') as
    select client, server, floor(client/server, 3) p, query
    from query_time where p > 1.02 order by p desc;

create table test_time engine Memory as
    select test, sum(client) total_client_time,
        maxIf(client, not short) query_max,
        minIf(client, not short) query_min,
        count(*) queries,
        sum(short) short_queries
    from query_time full join queries
    using test, query
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

create table all_tests_tsv engine File(TSV, 'report/all-queries.tsv') as
    select changed_fail, unstable_fail,
        left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold, test, query
    from queries order by test, query;
" 2> >(tee -a report/errors.log 1>&2)

for x in {right,left}-{addresses,{query,query-thread,trace,metric}-log}.tsv
do
    # FIXME This loop builds column definitons from TSVWithNamesAndTypes in an
    # absolutely atrocious way. This should be done by the file() function itself.
    paste -d' ' \
        <(sed -n '1{s/\t/\n/g;p;q}' "$x" | sed 's/\(^.*$\)/"\1"/') \
        <(sed -n '2{s/\t/\n/g;p;q}' "$x" ) \
        | tr '\n' ', ' | sed 's/,$//' > "$x.columns"
done

for version in {right,left}
do
clickhouse-local --query "
create view queries_for_flamegraph as
    select * from file('report/queries-for-flamegraph.tsv', TSVWithNamesAndTypes,
        'query text, test text');

create view query_log as select *
    from file('$version-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-query-log.tsv.columns")');

create view trace_log as select *
    from file('$version-trace-log.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-trace-log.tsv.columns")');

create view addresses_src as select *
    from file('$version-addresses.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-addresses.tsv.columns")');

create table addresses_join_$version engine Join(any, left, address) as
    select addr address, name from addresses_src;

create table unstable_query_runs engine File(TSVWithNamesAndTypes,
        'unstable-query-runs.$version.rep') as
    select query, query_id from query_log
    where query in (select query from queries_for_flamegraph)
        and query_id not like 'prewarm %'
    ;

create table unstable_query_log engine File(Vertical,
        'unstable-query-log.$version.rep') as
    select * from query_log
    where query_id in (select query_id from unstable_query_runs);

create table unstable_run_metrics engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics.$version.rep') as
    select ProfileEvents.Values value, ProfileEvents.Names metric, query_id, query
    from query_log array join ProfileEvents
    where query_id in (select query_id from unstable_query_runs)
    ;

create table unstable_run_metrics_2 engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics-2.$version.rep') as
    select v, n, query_id, query
    from
        (select
            ['memory_usage', 'read_bytes', 'written_bytes', 'query_duration_ms'] n,
            [memory_usage, read_bytes, written_bytes, query_duration_ms] v,
            query,
            query_id
        from query_log
        where query_id in (select query_id from unstable_query_runs))
    array join n, v;

create table unstable_run_traces engine File(TSVWithNamesAndTypes,
        'unstable-run-traces.$version.rep') as
    select
        count() value,
        joinGet(addresses_join_$version, 'name', arrayJoin(trace)) metric,
        unstable_query_runs.query_id,
        any(unstable_query_runs.query) query
    from unstable_query_runs
    join trace_log on trace_log.query_id = unstable_query_runs.query_id
    group by unstable_query_runs.query_id, metric
    order by count() desc
    ;

create table metric_devation engine File(TSVWithNamesAndTypes,
        'metric-deviation.$version.rep') as
    select query, floor((q[3] - q[1])/q[2], 3) d,
        quantilesExact(0, 0.5, 1)(value) q, metric
    from (select * from unstable_run_metrics
        union all select * from unstable_run_traces
        union all select * from unstable_run_metrics_2) mm
    join queries_for_flamegraph using query
    group by query, metric
    having d > 0.5
    order by query desc, d desc
    ;

create table stacks engine File(TSV, 'stacks.$version.rep') as
    select
        query,
        arrayStringConcat(
            arrayMap(x -> joinGet(addresses_join_$version, 'name', x),
                arrayReverse(trace)
            ),
            ';'
        ) readable_trace,
        count()
    from trace_log
    join unstable_query_runs using query_id
    group by query, trace
    ;
" 2> >(tee -a report/errors.log 1>&2) # do not run in parallel because they use the same data dir for StorageJoins which leads to weird errors.
done
wait

IFS=$'\n'
for version in {right,left}
do
    for query in $(cut -d'	' -f1 "stacks.$version.rep" | sort | uniq)
    do
        query_file=$(echo "$query" | cut -c-120 | sed 's/[/]/_/g')

        # Build separate .svg flamegraph for each query.
        grep -F "$query	" "stacks.$version.rep" \
            | cut -d'	' -f 2- \
            | sed 's/\t/ /g' \
            | tee "$query_file.stacks.$version.rep" \
            | ~/fg/flamegraph.pl > "$query_file.$version.svg" &

        # Copy metric stats into separate files as well.
        grep -F "$query	" "metric-deviation.$version.rep" \
            | cut -f2- > "$query_file.$version.metrics.rep" &
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
    ( time get_profiles || restart || get_profiles ||: )

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
