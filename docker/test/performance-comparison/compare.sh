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

    # When testing commits from master, use the older test files. This allows the
    # tests to pass even when we add new functions and tests for them, that are
    # not supported in the old revision.
    # When testing a PR, use the test files from the PR so that we can test their
    # changes.
    test_prefix=$([ "$PR_TO_TEST" == "0" ] && echo left || echo right)/performance

    for x in {test-times,skipped-tests,wall-clock-times}.tsv
    do
        rm -v "$x" ||:
        touch "$x"
    done


    # FIXME a quick crutch to bring the run time down for the unstable tests --
    # if some performance tests xmls were changed in a PR, run only these ones.
    if [ "$PR_TO_TEST" != "0" ]
    then
        # changed-test.txt prepared in entrypoint.sh from git diffs, because it
        # has the cloned repo. Used to use rsync for that but it was really ugly
        # and not always correct (e.g. when the reference SHA is really old and
        # has some other differences to the tested SHA, besides the one introduced
        # by the PR).
        test_files_override=$(sed "s/tests\/performance/${test_prefix//\//\\/}/" changed-tests.txt)
        if [ "$test_files_override" != "" ]
        then
            test_files=$test_files_override
        fi
    fi

    # Run only explicitly specified tests, if any
    if [ -v CHPC_TEST_GREP ]
    then
        test_files=$(ls "$test_prefix" | grep "$CHPC_TEST_GREP" | xargs -I{} -n1 readlink -f "$test_prefix/{}")
    fi

    if [ "$test_files" == "" ]
    then
        # FIXME remove some broken long tests
        for test_name in {IPv4,IPv6,modulo,parse_engine_file,number_formatting_formats,select_format,arithmetic,cryptographic_hashes,logical_functions_{medium,small}}
        do
            printf "%s\tMarked as broken (see compare.sh)\n" "$test_name">> skipped-tests.tsv
            rm "$test_prefix/$test_name.xml" ||:
        done
        test_files=$(ls "$test_prefix"/*.xml)
    fi

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

        grep ^query "$test_name-raw.tsv" | cut -f2- > "$test_name-queries.tsv"
        grep ^client-time "$test_name-raw.tsv" | cut -f2- > "$test_name-client-time.tsv"
        skipped=$(grep ^skipped "$test_name-raw.tsv" | cut -f2-)
        if [ "$skipped" != "" ]
        then
            printf "%s\t%s\n" "$test_name" "$skipped">> skipped-tests.tsv
        fi
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
        gdb -p $pid --batch --ex "info proc all" --ex "thread apply all bt" --ex quit &> "$pid.gdb.log" &
    done
    wait

    for i in {1..10}
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
    find . -maxdepth 1 -name "*-queries.tsv" -print0 | \
        xargs -0 -n1 -I% basename % -queries.tsv | \
        parallel --verbose clickhouse-local --file "{}-queries.tsv" \
            --structure "\"query text, run int, version UInt32, time float\"" \
            --query "\"$(cat "$script_dir/eqmed.sql")\"" \
            ">" {}-report.tsv
}

# Analyze results
function report
{

for x in {right,left}-{addresses,{query,query-thread,trace,metric}-log}.tsv
do
    # FIXME This loop builds column definitons from TSVWithNamesAndTypes in an
    # absolutely atrocious way. This should be done by the file() function itself.
    paste -d' ' \
        <(sed -n '1{s/\t/\n/g;p;q}' "$x" | sed 's/\(^.*$\)/"\1"/') \
        <(sed -n '2{s/\t/\n/g;p;q}' "$x" ) \
        | tr '\n' ', ' | sed 's/,$//' > "$x.columns"
done

rm ./*.{rep,svg} test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv ||:

cat profile-errors.log >> report-errors.rep

clickhouse-local --query "
create table queries engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select
        -- FIXME Comparison mode doesn't make sense for queries that complete
        -- immediately, so for now we pretend they don't exist. We don't want to
        -- remove them altogether because we want to be able to detect regressions,
        -- but the right way to do this is not yet clear.
        (left + right) / 2 < 0.02 as short,

        -- Difference > 5% and > rd(99%) -- changed. We can't filter out flaky
        -- queries by rd(5%), because it can be zero when the difference is smaller
        -- than a typical distribution width. The difference is still real though.
        not short and abs(diff) > 0.05 and abs(diff) > rd[4] as changed,
        
        -- Not changed but rd(99%) > 5% -- unstable.
        not short and not changed and rd[4] > 0.05 as unstable,
        
        left, right, diff, rd,
        replaceAll(_file, '-report.tsv', '') test,

        -- Truncate long queries.
        if(length(query) < 300, query, substr(query, 1, 298) || '...') query
    from file('*-report.tsv', TSV, 'left float, right float, diff float, rd Array(float), query text');

create table changed_perf_tsv engine File(TSV, 'changed-perf.tsv') as
    select left, right, diff, rd, test, query from queries where changed
    order by abs(diff) desc;

create table unstable_queries_tsv engine File(TSV, 'unstable-queries.tsv') as
    select left, right, diff, rd, test, query from queries where unstable
    order by rd[4] desc;

create table unstable_tests_tsv engine File(TSV, 'bad-tests.tsv') as
    select test, sum(unstable) u, sum(changed) c, u + c s from queries
    group by test having s > 0 order by s desc;

create table query_time engine Memory as select *, replaceAll(_file, '-client-time.tsv', '') test
    from file('*-client-time.tsv', TSV, 'query text, client float, server float');

create table wall_clock engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

create table slow_on_client_tsv engine File(TSV, 'slow-on-client.tsv') as
    select client, server, floor(client/server, 3) p, query
    from query_time where p > 1.02 order by p desc;

create table test_time engine Memory as
    select test, sum(client) total_client_time,
        maxIf(client, not short) query_max,
        minIf(client, not short) query_min,
        count(*) queries,
        sum(short) short_queries
    from query_time, queries
    where query_time.query = queries.query
    group by test;

create table test_times_tsv engine File(TSV, 'test-times.tsv') as
    select wall_clock.test, real,
        floor(total_client_time, 3),
        queries,
        short_queries,
        floor(query_max, 3),
        floor(real / queries, 3) avg_real_per_query,
        floor(query_min, 3)
    from test_time join wall_clock using test
    order by avg_real_per_query desc;

create table all_tests_tsv engine File(TSV, 'all-queries.tsv') as
    select left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        rd, test, query
    from queries order by test, query;
" 2> >(head -2 >> report-errors.rep) ||:

for version in {right,left}
do
clickhouse-local --query "
create view queries as
    select * from file('queries.rep', TSVWithNamesAndTypes,
        'short int, changed int, unstable int, left float, right float,
            diff float, rd Array(float), test text, query text');

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
    select query_id, query from query_log
    join queries using query
    where query_id not like 'prewarm %' and (unstable or changed)
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
    join queries using query
    group by query, metric
    having d > 0.5
    order by any(rd[3]) desc, query desc, d desc
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
" 2> >(head -2 >> report-errors.rep) ||: # do not run in parallel because they use the same data dir for StorageJoins which leads to weird errors.
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

    time "$script_dir/report.py" --report=all-queries > all-queries.html 2> >(head -2 >> report-errors.rep) ||:
    time "$script_dir/report.py" > report.html
    ;&
esac

# Print some final debug info to help debug Weirdness, of which there is plenty.
jobs
pstree -apgT
