#!/bin/bash
set -exu
set -o pipefail
trap "exit" INT TERM
# The watchdog is in the separate process group, so we have to kill it separately
# if the script terminates earlier.
trap 'kill $(jobs -pr) ${watchdog_pid:-} ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# upstream/master
LEFT_SERVER_PORT=9001
# patched version
RIGHT_SERVER_PORT=9002

function wait_for_server # port, pid
{
    for _ in {1..60}
    do
        if clickhouse-client --port "$1" --query "select 1" || ! kill -0 "$2"
        then
            break
        fi
        sleep 1
    done

    if ! clickhouse-client --port "$1" --query "select 1"
    then
        echo "Cannot connect to ClickHouse server at $1"
        return 1
    fi

    if ! kill -0 "$2"
    then
        echo "Server pid '$2' is not running"
        return 1
    fi
}

function left_or_right()
{
    local from=$1 && shift
    local basename=$1 && shift

    if [ -e "$from/$basename" ]; then
        echo "$from/$basename"
        return
    fi

    case "$from" in
        left) echo "right/$basename" ;;
        right) echo "left/$basename" ;;
    esac
}

function configure
{
    # Use the new config for both servers, so that we can change it in a PR.
    rm right/config/config.d/text_log.xml ||:
    cp -rv right/config left ||:

    # Start a temporary server to rename the tables
    while pkill clickhouse-serv; do echo . ; sleep 1 ; done
    echo all killed

    set -m # Spawn temporary in its own process groups

    local setup_left_server_opts=(
        # server options
        --config-file=left/config/config.xml
        --
        # server *config* directives overrides
        --path db0
        --user_files_path db0/user_files
        --top_level_domains_path "$(left_or_right right top_level_domains)"
        --tcp_port $LEFT_SERVER_PORT
    )
    left/clickhouse-server "${setup_left_server_opts[@]}" &> setup-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid
    set +m

    wait_for_server $LEFT_SERVER_PORT $left_pid
    echo Server for setup started

    clickhouse-client --port $LEFT_SERVER_PORT --query "create database test" ||:
    clickhouse-client --port $LEFT_SERVER_PORT --query "rename table datasets.hits_v1 to test.hits" ||:

    while pkill clickhouse-serv; do echo . ; sleep 1 ; done
    echo all killed

    # Make copies of the original db for both servers. Use hardlinks instead
    # of copying to save space. Before that, remove preprocessed configs and
    # system tables, because sharing them between servers with hardlinks may
    # lead to weird effects.
    rm -r left/db ||:
    rm -r right/db ||:
    rm -r db0/preprocessed_configs ||:
    rm -r db0/{data,metadata}/system ||:
    rm db0/status ||:
    cp -al db0/ left/db/
    cp -al db0/ right/db/
}

function restart
{
    while pkill clickhouse-serv; do echo . ; sleep 1 ; done
    echo all killed

    # Change the jemalloc settings here.
    # https://github.com/jemalloc/jemalloc/wiki/Getting-Started
    export MALLOC_CONF="confirm_conf:true"

    set -m # Spawn servers in their own process groups

    local left_server_opts=(
        # server options
        --config-file=left/config/config.xml
        --
        # server *config* directives overrides
        --path left/db
        --user_files_path left/db/user_files
        --top_level_domains_path "$(left_or_right left top_level_domains)"
        --tcp_port $LEFT_SERVER_PORT
    )
    left/clickhouse-server "${left_server_opts[@]}" &>> left-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid

    local right_server_opts=(
        # server options
        --config-file=right/config/config.xml
        --
        # server *config* directives overrides
        --path right/db
        --user_files_path right/db/user_files
        --top_level_domains_path "$(left_or_right right top_level_domains)"
        --tcp_port $RIGHT_SERVER_PORT
    )
    right/clickhouse-server "${right_server_opts[@]}" &>> right-server-log.log &
    right_pid=$!
    kill -0 $right_pid
    disown $right_pid

    set +m

    unset MALLOC_CONF

    wait_for_server $LEFT_SERVER_PORT $left_pid
    echo left ok

    wait_for_server $RIGHT_SERVER_PORT $right_pid
    echo right ok

    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.tables where database != 'system'"
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.build_options"
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.tables where database != 'system'"
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.build_options"

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
    elif [ "$PR_TO_TEST" == "0" ]
    then
        # When testing commits from master, use the older test files. This
        # allows the tests to pass even when we add new functions and tests for
        # them, that are not supported in the old revision.
        test_prefix=left/performance
    else
        # For PRs, use newer test files so we can test these changes.
        test_prefix=right/performance
    fi

    # Determine which tests to run.
    if [ -v CHPC_TEST_GREP ]
    then
        # Run only explicitly specified tests, if any.
        # shellcheck disable=SC2010
        test_files=($(ls "$test_prefix" | grep "$CHPC_TEST_GREP" | xargs -I{} -n1 readlink -f "$test_prefix/{}"))
    elif [ "$PR_TO_TEST" -ne 0 ] \
        && [ "$(wc -l < changed-test-definitions.txt)" -gt 0 ] \
        && [ "$(wc -l < other-changed-files.txt)" -eq 0 ]
    then
        # If only the perf tests were changed in the PR, we will run only these
        # tests. The lists of changed files are prepared in entrypoint.sh because
        # it has the repository.
        test_files=($(sed "s/tests\/performance/${test_prefix//\//\\/}/" changed-test-definitions.txt))
    else
        # The default -- run all tests found in the test dir.
        test_files=($(ls "$test_prefix"/*.xml))
    fi

    # We split perf tests into multiple checks to make them faster
    if [ -v CHPC_TEST_RUN_BY_HASH_TOTAL ]; then
        # filter tests array in bash https://stackoverflow.com/a/40375567
        for index in "${!test_files[@]}"; do
            # sorry for this, just calculating hash(test_name) % total_tests_group == my_test_group_num
            test_hash_result=$(echo test_files[$index] | perl -ne 'use Digest::MD5 qw(md5); print unpack('Q', md5($_)) % $ENV{CHPC_TEST_RUN_BY_HASH_TOTAL} == $ENV{CHPC_TEST_RUN_BY_HASH_NUM};')
            # BTW, for some reason when hash(test_name) % total_tests_group != my_test_group_num perl outputs nothing, not zero
            if [ "$test_hash_result" != "1" ]; then
                # deleting element from array
                unset -v 'test_files[$index]'
            fi
        done
        # to have sequential indexes...
        test_files=("${test_files[@]}")
    fi

    # For PRs w/o changes in test definitons, test only a subset of queries,
    # and run them less times. If the corresponding environment variables are
    # already set, keep those values.
    #
    # NOTE: too high CHPC_RUNS/CHPC_MAX_QUERIES may hit internal CI timeout.
    # NOTE: Currently we disabled complete run even for master branch
    #if [ "$PR_TO_TEST" -ne 0 ] && [ "$(wc -l < changed-test-definitions.txt)" -eq 0 ]
    #then
    #    CHPC_RUNS=${CHPC_RUNS:-7}
    #    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-10}
    #else
    #    CHPC_RUNS=${CHPC_RUNS:-13}
    #    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-0}
    #fi

    CHPC_RUNS=${CHPC_RUNS:-7}
    CHPC_MAX_QUERIES=${CHPC_MAX_QUERIES:-10}

    export CHPC_RUNS
    export CHPC_MAX_QUERIES

    # Determine which concurrent benchmarks to run. For now, the only test
    # we run as a concurrent benchmark is 'website'. Run it as benchmark if we
    # are also going to run it as a normal test.
    for test in ${test_files[@]}; do echo "$test"; done | sed -n '/website/p' > benchmarks-to-run.txt

    # Delete old report files.
    for x in {test-times,wall-clock-times}.tsv
    do
        rm -v "$x" ||:
        touch "$x"
    done

    # Randomize test order. BTW, it's not an array no more.
    test_files=$(for f in ${test_files[@]}; do echo "$f"; done | sort -R)

    # Limit profiling time to 10 minutes, not to run for too long.
    profile_seconds_left=600

    # Run the tests.
    total_tests=$(echo "$test_files" | wc -w)
    current_test=0
    test_name="<none>"
    for test in $test_files
    do
        echo "$current_test of $total_tests tests complete" > status.txt
        # Check that both servers are alive, and restart them if they die.
        clickhouse-client --port $LEFT_SERVER_PORT --query "select 1 format Null" \
            || { echo $test_name >> left-server-died.log ; restart ; }
        clickhouse-client --port $RIGHT_SERVER_PORT --query "select 1 format Null" \
            || { echo $test_name >> right-server-died.log ; restart ; }

        test_name=$(basename "$test" ".xml")
        echo test "$test_name"

        # Don't profile if we're past the time limit.
        # Use awk because bash doesn't support floating point arithmetic.
        profile_seconds=$(awk "BEGIN { print ($profile_seconds_left > 0 ? 10 : 0) }")

        (
            set +x
            argv=(
                --host localhost localhost
                --port "$LEFT_SERVER_PORT" "$RIGHT_SERVER_PORT"
                --runs "$CHPC_RUNS"
                --max-queries "$CHPC_MAX_QUERIES"
                --profile-seconds "$profile_seconds"

                "$test"
            )
            TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
            # one more subshell to suppress trace output for "set +x"
            (
                time "$script_dir/perf.py" "${argv[@]}" > "$test_name-raw.tsv" 2> "$test_name-err.log"
            ) 2>>wall-clock-times.tsv >/dev/null \
                || echo "Test $test_name failed with error code $?" >> "$test_name-err.log"
        ) 2>/dev/null

        profile_seconds_left=$(awk -F'	' \
            'BEGIN { s = '$profile_seconds_left'; } /^profile-total/ { s -= $2 } END { print s }' \
            "$test_name-raw.tsv")
        current_test=$((current_test + 1))
    done

    wait
}

function get_profiles_watchdog
{
    sleep 600

    echo "The trace collection did not finish in time." >> profile-errors.log

    for pid in $(pgrep -f clickhouse)
    do
        sudo gdb -p "$pid" --batch --ex "info proc all" --ex "thread apply all bt" --ex quit &> "$pid.gdb.log" &
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
    clickhouse-client --port $LEFT_SERVER_PORT --query "system flush logs" &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "system flush logs" &

    wait

    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.query_log where type in ('QueryFinish', 'ExceptionWhileProcessing') format TSVWithNamesAndTypes" > left-query-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.query_thread_log format TSVWithNamesAndTypes" > left-query-thread-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.trace_log format TSVWithNamesAndTypes" > left-trace-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > left-addresses.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.metric_log format TSVWithNamesAndTypes" > left-metric-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.asynchronous_metric_log format TSVWithNamesAndTypes" > left-async-metric-log.tsv ||: &

    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.query_log where type in ('QueryFinish', 'ExceptionWhileProcessing') format TSVWithNamesAndTypes" > right-query-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.query_thread_log format TSVWithNamesAndTypes" > right-query-thread-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.trace_log format TSVWithNamesAndTypes" > right-trace-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > right-addresses.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.metric_log format TSVWithNamesAndTypes" > right-metric-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.asynchronous_metric_log format TSVWithNamesAndTypes" > right-async-metric-log.tsv ||: &

    wait

    # Just check that the servers are alive so that we return a proper exit code.
    # We don't consistently check the return codes of the above background jobs.
    clickhouse-client --port $LEFT_SERVER_PORT --query "select 1"
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select 1"
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
# To debug calculations only for a particular test, substitute a suitable
# wildcard here, e.g. `for test_file in modulo-raw.tsv`.
for test_file in *-raw.tsv
do
    test_name=$(basename "$test_file" "-raw.tsv")
    sed -n "s/^query\t/$test_name\t/p" < "$test_file" >> "analyze/query-runs.tsv"
    sed -n "s/^profile\t/$test_name\t/p" < "$test_file" >> "analyze/query-profiles.tsv"
    sed -n "s/^client-time\t/$test_name\t/p" < "$test_file" >> "analyze/client-times.tsv"
    sed -n "s/^report-threshold\t/$test_name\t/p" < "$test_file" >> "analyze/report-thresholds.tsv"
    sed -n "s/^skipped\t/$test_name\t/p" < "$test_file" >> "analyze/skipped-tests.tsv"
    sed -n "s/^display-name\t/$test_name\t/p" < "$test_file" >> "analyze/query-display-names.tsv"
    sed -n "s/^short\t/$test_name\t/p" < "$test_file" >> "analyze/marked-short-queries.tsv"
    sed -n "s/^partial\t/$test_name\t/p" < "$test_file" >> "analyze/partial-queries.tsv"
done

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
    select 0 version, query_id, ProfileEvents,
        query_duration_ms, memory_usage from left_query_log
    union all
    select 1 version, query_id, ProfileEvents,
        query_duration_ms, memory_usage from right_query_log
    ;

-- This is a single source of truth on all metrics we have for query runs. The
-- metrics include ProfileEvents from system.query_log, and query run times
-- reported by the perf.py test runner.
create table query_run_metric_arrays engine File(TSV, 'analyze/query-run-metric-arrays.tsv')
    as
    with (
        -- sumMapState with the list of all keys with '-0.' values. Negative zero is because
        -- sumMap removes keys with positive zeros.
        with (select groupUniqArrayArray(mapKeys(ProfileEvents)) from query_logs) as all_names
            select arrayReduce('sumMapState', [(all_names, arrayMap(x->-0., all_names))])
        ) as all_metrics
    select test, query_index, version, query_id,
        (finalizeAggregation(
            arrayReduce('sumMapMergeState',
                [
                    all_metrics,
                    arrayReduce('sumMapState',
                        [(mapKeys(ProfileEvents),
                            arrayMap(x->toFloat64(x), mapValues(ProfileEvents)))]
                    ),
                    arrayReduce('sumMapState', [(
                        ['client_time', 'server_time', 'memory_usage'],
                        arrayMap(x->if(x != 0., x, -0.), [
                            toFloat64(query_runs.time),
                            toFloat64(query_duration_ms / 1000.),
                            toFloat64(memory_usage)]))])
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

-- Filter out tests that don't have an even number of runs, to avoid breaking
-- the further calculations. This may happen if there was an error during the
-- test runs, e.g. the server died. It will be reported in test errors, so we
-- don't have to report it again.
create view broken_queries as
    select test, query_index
    from query_runs
    group by test, query_index
    having count(*) % 2 != 0
    ;

-- This is for statistical processing with eqmed.sql
create table query_run_metrics_for_stats engine File(
        TSV, -- do not add header -- will parse with grep
        'analyze/query-run-metrics-for-stats.tsv')
    as select test, query_index, 0 run, version,
        -- For debugging, add a filter for a particular metric like this:
        -- arrayFilter(m, n -> n = 'client_time', metric_values, metric_names)
        --     metric_values
        -- Note that further reporting may break, because the metric names are
        -- not filtered.
        metric_values
    from query_run_metric_arrays
    where (test, query_index) not in broken_queries
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
    file="analyze/tmp/${prefix//	/_}.tsv"
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

# The comparison script might be bound to one NUMA node for better test
# stability, and the calculation runs out of memory because of this. Use
# all nodes.
numactl --show
numactl --cpunodebind=all --membind=all numactl --show
# Use less jobs to avoid OOM. Some queries can consume 8+ GB of memory.
jobs_count=$(($(grep -c ^processor /proc/cpuinfo) / 3))
numactl --cpunodebind=all --membind=all parallel --jobs  $jobs_count --joblog analyze/parallel-log.txt --null < analyze/commands.txt 2>> analyze/errors.log

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

# Fetch historical query variability thresholds from the CI database
if [ -v CHPC_DATABASE_URL ]
then
    set +x # Don't show password in the log
    client=(clickhouse-client
        # Surprisingly, clickhouse-client doesn't understand --host 127.0.0.1:9000
        # so I have to extract host and port with clickhouse-local. I tried to use
        # Poco URI parser to support this in the client, but it's broken and can't
        # parse host:port.
        $(clickhouse-local --query "with '${CHPC_DATABASE_URL}' as url select '--host ' || domain(url) || ' --port ' || toString(port(url)) format TSV")
        --secure
        --user "${CHPC_DATABASE_USER}"
        --password "${CHPC_DATABASE_PASSWORD}"
        --config "right/config/client_config.xml"
        --database perftest
        --date_time_input_format=best_effort)


# Precision is going to be 1.5 times worse for PRs, because we run the queries
# less times. How do I know it? I ran this:
# SELECT quantilesExact(0., 0.1, 0.5, 0.75, 0.95, 1.)(p / m)
# FROM
# (
#     SELECT
#         quantileIf(0.95)(stat_threshold, pr_number = 0) AS m,
#         quantileIf(0.95)(stat_threshold, (pr_number != 0) AND (abs(diff) < stat_threshold)) AS p
#     FROM query_metrics_v2
#     WHERE (event_date > (today() - toIntervalMonth(1))) AND (metric = 'client_time')
#     GROUP BY
#         test,
#         query_index,
#         query_display_name
#     HAVING count(*) > 100
# )
#
# The file can be empty if the server is inaccessible, so we can't use
# TSVWithNamesAndTypes.
#
    "${client[@]}" --query "
            select test, query_index,
                quantileExact(0.99)(abs(diff)) * 1.5 AS max_diff,
                quantileExactIf(0.99)(stat_threshold, abs(diff) < stat_threshold) * 1.5 AS max_stat_threshold,
                query_display_name
            from query_metrics_v2
            -- We use results at least one week in the past, so that the current
            -- changes do not immediately influence the statistics, and we have
            -- some time to notice that something is wrong.
            where event_date between now() - interval 1 month - interval 1 week
                    and now() - interval 1 week
                and metric = 'client_time'
                and pr_number = 0
            group by test, query_index, query_display_name
            having count(*) > 100
            " > analyze/historical-thresholds.tsv
    set -x
else
    touch analyze/historical-thresholds.tsv
fi

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

create view partial_query_times as select * from
    file('analyze/partial-query-times.tsv', TSVWithNamesAndTypes,
        'test text, query_index int, time_stddev float, time_median double')
    ;

-- Report for partial queries that we could only run on the new server (e.g.
-- queries with new functions added in the tested PR).
create table partial_queries_report engine File(TSV, 'report/partial-queries-report.tsv')
    settings output_format_decimal_trailing_zeros = 1
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

create table report_thresholds engine File(TSVWithNamesAndTypes, 'report/thresholds.tsv')
    as select
        query_display_names.test test, query_display_names.query_index query_index,
        ceil(greatest(0.1, historical_thresholds.max_diff,
            test_thresholds.report_threshold), 2) changed_threshold,
        ceil(greatest(0.2, historical_thresholds.max_stat_threshold,
            test_thresholds.report_threshold + 0.1), 2) unstable_threshold,
        query_display_names.query_display_name query_display_name
    from query_display_names
    left join file('analyze/historical-thresholds.tsv', TSV,
        'test text, query_index int, max_diff float, max_stat_threshold float,
            query_display_name text') historical_thresholds
    on query_display_names.test = historical_thresholds.test
        and query_display_names.query_index = historical_thresholds.query_index
        and query_display_names.query_display_name = historical_thresholds.query_display_name
    left join file('analyze/report-thresholds.tsv', TSV,
        'test text, report_threshold float') test_thresholds
    on query_display_names.test = test_thresholds.test
    ;

-- Main statistics for queries -- query time as reported in query log.
create table queries engine File(TSVWithNamesAndTypes, 'report/queries.tsv')
    as select
        -- It is important to have a non-strict inequality with stat_threshold
        -- here. The randomization distribution is actually discrete, and when
        -- the number of runs is small, the quantile we need (e.g. 0.99) turns
        -- out to be the maximum value of the distribution. We can also hit this
        -- maximum possible value with our test run, and this obviously means
        -- that we have observed the difference to the best precision possible
        -- for the given number of runs. If we use a strict equality here, we
        -- will miss such cases. This happened in the wild and lead to some
        -- uncaught regressions, because for the default 7 runs we do for PRs,
        -- the randomization distribution has only 16 values, so the max quantile
        -- is actually 0.9375.
        abs(diff) > changed_threshold        and abs(diff) >= stat_threshold as changed_fail,
        abs(diff) > changed_threshold - 0.05 and abs(diff) >= stat_threshold as changed_show,

        not changed_fail and stat_threshold > unstable_threshold as unstable_fail,
        not changed_show and stat_threshold > unstable_threshold - 0.05 as unstable_show,

        left, right, diff, stat_threshold,
        query_metric_stats.test test, query_metric_stats.query_index query_index,
        query_display_names.query_display_name query_display_name
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    left join report_thresholds
        on query_display_names.test = report_thresholds.test
            and query_display_names.query_index = report_thresholds.query_index
            and query_display_names.query_display_name = report_thresholds.query_display_name
    -- 'server_time' is rounded down to ms, which might be bad for very short queries.
    -- Use 'client_time' instead.
    where metric_name = 'client_time'
    order by test, query_index, metric_name
    ;

create table changed_perf_report engine File(TSV, 'report/changed-perf.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        toDecimal64(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select
        toDecimal64(left, 3), toDecimal64(right, 3), times_change_str,
        toDecimal64(diff, 3), toDecimal64(stat_threshold, 3),
        changed_fail, test, query_index, query_display_name
    from queries where changed_show order by abs(diff) desc;

create table unstable_queries_report engine File(TSV, 'report/unstable-queries.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select
        toDecimal64(left, 3), toDecimal64(right, 3), toDecimal64(diff, 3),
        toDecimal64(stat_threshold, 3), unstable_fail, test, query_index, query_display_name
    from queries where unstable_show order by stat_threshold desc;


create view test_speedup as
    select
        test,
        exp2(avg(log2(left / right))) times_speedup,
        count(*) queries,
        unstable + changed bad,
        sum(changed_show) changed,
        sum(unstable_show) unstable
    from queries
    group by test
    order by times_speedup desc
    ;

create view total_speedup as
    select
        'Total' test,
        exp2(avg(log2(times_speedup))) times_speedup,
        sum(queries) queries,
        unstable + changed bad,
        sum(changed) changed,
        sum(unstable) unstable
    from test_speedup
    ;

create table test_perf_changes_report engine File(TSV, 'report/test-perf-changes.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        (times_speedup >= 1
            ? '-' || toString(toDecimal64(times_speedup, 3)) || 'x'
            : '+' || toString(toDecimal64(1 / times_speedup, 3)) || 'x')
        as times_speedup_str
    select test, times_speedup_str, queries, bad, changed, unstable
    -- Not sure what's the precedence of UNION ALL vs WHERE & ORDER BY, hence all
    -- the braces.
    from (
        (
            select * from total_speedup
        ) union all (
            select * from test_speedup
            where
                (times_speedup >= 1 ? times_speedup : (1 / times_speedup)) >= 1.005
                or bad
        )
    )
    order by test = 'Total' desc, times_speedup desc
    ;


create view total_client_time_per_query as select *
    from file('analyze/client-times.tsv', TSV,
        'test text, query_index int, client float, server float');

create table slow_on_client_report engine File(TSV, 'report/slow-on-client.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select client, server, toDecimal64(client/server, 3) p,
        test, query_display_name
    from total_client_time_per_query left join query_display_names using (test, query_index)
    where p > toDecimal64(1.02, 3) order by p desc;

create table wall_clock_time_per_test engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float, user float, system float');

create table test_time engine Memory as
    select test, sum(client) total_client_time,
        max(client) query_max,
        min(client) query_min,
        count(*) queries
    from total_client_time_per_query full join queries using (test, query_index)
    group by test;

create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

--
-- Guess the number of query runs used for this test. The number is required to
-- calculate and check the average query run time in the report.
-- We have to be careful, because we will encounter:
--  1) partial queries which run only on one server
--  2) short queries which run for a much higher number of times
--  3) some errors that make query run for a different number of times on a
--     particular server.
--
create view test_runs as
    select test,
        -- Default to 7 runs if there are only 'short' queries in the test, and
        -- we can't determine the number of runs.
        if((ceil(medianOrDefaultIf(t.runs, not short), 0) as r) != 0, r, 7) runs
    from (
        select
            -- The query id is the same for both servers, so no need to divide here.
            uniqExact(query_id) runs,
            (test, query_index) in
                (select * from file('analyze/marked-short-queries.tsv', TSV,
                    'test text, query_index int'))
            as short,
            test, query_index
        from query_runs
        group by test, query_index
        ) t
    group by test
    ;

create view test_times_view as
    select
        wall_clock_time_per_test.test test,
        real,
        total_client_time,
        queries,
        query_max,
        real / if(queries > 0, queries, 1) avg_real_per_query,
        query_min,
        runs
    from test_time
        -- wall clock times are also measured for skipped tests, so don't
        -- do full join
        left join wall_clock_time_per_test
            on wall_clock_time_per_test.test = test_time.test
        full join test_runs
            on test_runs.test = test_time.test
    ;

-- WITH TOTALS doesn't work with INSERT SELECT, so we have to jump through these
-- hoops: https://github.com/ClickHouse/ClickHouse/issues/15227
create view test_times_view_total as
    select
        'Total' test,
        sum(real),
        sum(total_client_time),
        sum(queries),
        max(query_max),
        sum(real) / if(sum(queries) > 0, sum(queries), 1) avg_real_per_query,
        min(query_min),
        -- Totaling the number of runs doesn't make sense, but use the max so
        -- that the reporting script doesn't complain about queries being too
        -- long.
        max(runs)
    from test_times_view
    ;

create table test_times_report engine File(TSV, 'report/test-times.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select
        test,
        toDecimal64(real, 3),
        toDecimal64(total_client_time, 3),
        queries,
        toDecimal64(query_max, 3),
        toDecimal64(avg_real_per_query, 3),
        toDecimal64(query_min, 3),
        runs
    from (
        select * from test_times_view
        union all
        select * from test_times_view_total
        )
    order by test = 'Total' desc, avg_real_per_query desc
    ;

-- report for all queries page, only main metric
create table all_tests_report engine File(TSV, 'report/all-queries.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        toDecimal64(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select changed_fail, unstable_fail,
        toDecimal64(left, 3), toDecimal64(right, 3), times_change_str,
        toDecimal64(isFinite(diff) ? diff : 0, 3),
        toDecimal64(isFinite(stat_threshold) ? stat_threshold : 0, 3),
        test, query_index, query_display_name
    from queries order by test, query_index;


-- Report of queries that have inconsistent 'short' markings:
-- 1) have short duration, but are not marked as 'short'
-- 2) the reverse -- marked 'short' but take too long.
-- The threshold for 2) is significantly larger than the threshold for 1), to
-- avoid jitter.
create view shortness
    as select
        (test, query_index) in
            (select * from file('analyze/marked-short-queries.tsv', TSV,
            'test text, query_index int'))
            as marked_short,
        time, test, query_index, query_display_name
    from (
            select right time, test, query_index from queries
            union all
            select time_median, test, query_index from partial_query_times
        ) times
        left join query_display_names
            on times.test = query_display_names.test
                and times.query_index = query_display_names.query_index
    ;

create table inconsistent_short_marking_report
    engine File(TSV, 'report/unexpected-query-duration.tsv')
    as select
        multiIf(marked_short and time > 0.1, '\"short\" queries must run faster than 0.02 s',
                not marked_short and time < 0.02, '\"normal\" queries must run longer than 0.1 s',
                '') problem,
        marked_short, time,
        test, query_index, query_display_name
    from shortness
    where problem != ''
    ;


--------------------------------------------------------------------------------
-- various compatibility data formats follow, not related to the main report

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select 0 short, changed_fail, unstable_fail, left, right, diff,
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

# Prepare source data for metrics and flamegraphs for queries that were profiled
# by perf.py.
for version in {right,left}
do
    rm -rf data
    clickhouse-local --query "
create view query_profiles as
    with 0 as left, 1 as right
    select * from file('analyze/query-profiles.tsv', TSV,
        'test text, query_index int, query_id text, version UInt8, time float')
    where version = $version
    ;

create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create table unstable_query_runs engine File(TSVWithNamesAndTypes,
        'unstable-query-runs.$version.rep') as
    select query_profiles.test test, query_profiles.query_index query_index,
        query_display_name, query_id
    from query_profiles
    left join query_display_names on
        query_profiles.test = query_display_names.test
        and query_profiles.query_index = query_display_names.query_index
    ;

create view query_log as select *
    from file('$version-query-log.tsv', TSVWithNamesAndTypes,
        '$(cat "$version-query-log.tsv.columns")');

create table unstable_run_metrics engine File(TSVWithNamesAndTypes,
        'unstable-run-metrics.$version.rep') as
    select test, query_index, query_id, value, metric
    from query_log
    array join
        mapValues(ProfileEvents) as value,
        mapKeys(ProfileEvents) as metric
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
        'report/metric-deviation.$version.tsv')
    settings output_format_decimal_trailing_zeros = 1
    -- first goes the key used to split the file with grep
    as select test, query_index, query_display_name,
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
touch report/query-files.txt
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
while IFS= read -r query_file
do
    ~/fg/difffolded.pl "report/tmp/$query_file.stacks.left.tsv" \
            "report/tmp/$query_file.stacks.right.tsv" \
        | tee "report/tmp/$query_file.stacks.diff.tsv" \
        | ~/fg/flamegraph.pl > "$query_file.diff.svg" &
done < report/query-files.txt
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
        # The second grep is a heuristic for error messages like
        # "socket.timeout: timed out".
        grep -h -m2 -i '\(Exception\|Error\):[^:]' "$log" \
            || grep -h -m2 -i '^[^ ]\+: ' "$log" \
            || head -2 "$log"
    } | sed "s/^/$test\t/" >> run-errors.tsv ||:
done
}

function report_metrics
{
build_log_column_definitions

rm -rf metrics ||:
mkdir metrics

clickhouse-local --query "
create view right_async_metric_log as
    select * from file('right-async-metric-log.tsv', TSVWithNamesAndTypes,
        '$(cat right-async-metric-log.tsv.columns)')
    ;

-- Use the right log as time reference because it may have higher precision.
create table metrics engine File(TSV, 'metrics/metrics.tsv') as
    with (select min(event_time) from right_async_metric_log) as min_time
    select metric, r.event_time - min_time event_time, l.value as left, r.value as right
    from right_async_metric_log r
    asof join file('left-async-metric-log.tsv', TSVWithNamesAndTypes,
        '$(cat left-async-metric-log.tsv.columns)') l
    on l.metric = r.metric and r.event_time <= l.event_time
    order by metric, event_time
    ;

-- Show metrics that have changed
create table changes engine File(TSV, 'metrics/changes.tsv')
    settings output_format_decimal_trailing_zeros = 1
    as select metric, left, right,
        toDecimal64(diff, 3), toDecimal64(times_diff, 3)
    from (
        select metric, median(left) as left, median(right) as right,
            (right - left) / left diff,
            if(left > right, left / right, right / left) times_diff
        from metrics
        group by metric
        having abs(diff) > 0.05 and isFinite(diff) and isFinite(times_diff)
    )
    order by diff desc
    ;
" 2> >(tee -a metrics/errors.log 1>&2)

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

function upload_results
{
    # Prepare info for the CI checks table.
    rm ci-checks.tsv
    clickhouse-local --query "
create view queries as select * from file('report/queries.tsv', TSVWithNamesAndTypes,
    'changed_fail int, changed_show int, unstable_fail int, unstable_show int,
        left float, right float, diff float, stat_threshold float,
        test text, query_index int, query_display_name text');

create table ci_checks engine File(TSVWithNamesAndTypes, 'ci-checks.tsv')
    as select
        $PR_TO_TEST pull_request_number,
        '$SHA_TO_TEST' commit_sha,
        'Performance' check_name,
        '$(sed -n 's/.*<!--status: \(.*\)-->/\1/p' report.html)' check_status,
        -- TODO toDateTime() can't parse output of 'date', so no time for now.
        ($(date +%s) - $CHPC_CHECK_START_TIMESTAMP) * 1000 check_duration_ms,
        fromUnixTimestamp($CHPC_CHECK_START_TIMESTAMP) check_start_time,
        test_name,
        test_status,
        test_duration_ms,
        report_url,
        $PR_TO_TEST = 0
            ? 'https://github.com/ClickHouse/ClickHouse/commit/$SHA_TO_TEST'
            : 'https://github.com/ClickHouse/ClickHouse/pull/$PR_TO_TEST' pull_request_url,
        '' commit_url,
        '' task_url,
        '' base_ref,
        '' base_repo,
        '' head_ref,
        '' head_repo
    from (
        select '' test_name,
            '$(sed -n 's/.*<!--message: \(.*\)-->/\1/p' report.html)' test_status,
            0 test_duration_ms,
            'https://clickhouse-test-reports.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/performance_comparison/report.html#fail1' report_url
        union all
            select test || ' #' || toString(query_index), 'slower' test_status, 0 test_duration_ms,
                'https://clickhouse-test-reports.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/performance_comparison/report.html#changes-in-performance.'
                    || test || '.' || toString(query_index) report_url
            from queries where changed_fail != 0 and diff > 0
        union all
            select test || ' #' || toString(query_index), 'unstable' test_status, 0 test_duration_ms,
                'https://clickhouse-test-reports.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/performance_comparison/report.html#unstable-queries.'
                    || test || '.' || toString(query_index) report_url
            from queries where unstable_fail != 0
    )
;
    "

    if ! [ -v CHPC_DATABASE_URL ]
    then
        echo Database for test results is not specified, will not upload them.
        return 0
    fi

    set +x # Don't show password in the log
    client=(clickhouse-client
        # Surprisingly, clickhouse-client doesn't understand --host 127.0.0.1:9000
        # so I have to extract host and port with clickhouse-local. I tried to use
        # Poco URI parser to support this in the client, but it's broken and can't
        # parse host:port.
        $(clickhouse-local --query "with '${CHPC_DATABASE_URL}' as url select '--host ' || domain(url) || ' --port ' || toString(port(url)) format TSV")
        --secure
        --user "${CHPC_DATABASE_USER}"
        --password "${CHPC_DATABASE_PASSWORD}"
        --config "right/config/client_config.xml"
        --database perftest
        --date_time_input_format=best_effort)

    "${client[@]}" --query "
            insert into query_metrics_v2
            select
                toDate(event_time) event_date,
                toDateTime('$(cd right/ch && git show -s --format=%ci "$SHA_TO_TEST" | cut -d' ' -f-2)') event_time,
                $PR_TO_TEST pr_number,
                '$REF_SHA' old_sha,
                '$SHA_TO_TEST' new_sha,
                test,
                query_index,
                query_display_name,
                metric_name,
                old_value,
                new_value,
                diff,
                stat_threshold
            from input('metric_name text, old_value float, new_value float, diff float,
                    ratio_display_text text, stat_threshold float,
                    test text, query_index int, query_display_name text')
            settings date_time_input_format='best_effort'
            format TSV
            settings date_time_input_format='best_effort'
" < report/all-query-metrics.tsv # Don't leave whitespace after INSERT: https://github.com/ClickHouse/ClickHouse/issues/16652

    # Upload some run attributes. I use this weird form because it is the same
    # form that can be used for historical data when you only have compare.log.
    cat compare.log \
        | sed -n '
            s/.*Model name:[[:space:]]\+\(.*\)$/metric	lscpu-model-name	\1/p;
            s/.*L1d cache:[[:space:]]\+\(.*\)$/metric	lscpu-l1d-cache	\1/p;
            s/.*L1i cache:[[:space:]]\+\(.*\)$/metric	lscpu-l1i-cache	\1/p;
            s/.*L2 cache:[[:space:]]\+\(.*\)$/metric	lscpu-l2-cache	\1/p;
            s/.*L3 cache:[[:space:]]\+\(.*\)$/metric	lscpu-l3-cache	\1/p;
            s/.*left_sha=\(.*\)$/old-sha	\1/p;
            s/.*right_sha=\(.*\)/new-sha	\1/p' \
        | awk '
            BEGIN { FS = "\t"; OFS = "\t" }
            /^old-sha/ { old_sha=$2 }
            /^new-sha/ { new_sha=$2 }
            /^metric/ { print old_sha, new_sha, $2, $3 }' \
        | "${client[@]}" --query "INSERT INTO run_attributes_v1 FORMAT TSV"

    # Grepping numactl results from log is too crazy, I'll just call it again.
    "${client[@]}" --query "INSERT INTO run_attributes_v1 FORMAT TSV" <<EOF
$REF_SHA	$SHA_TO_TEST	$(numactl --show | sed -n 's/^cpubind:[[:space:]]\+/numactl-cpubind	/p')
$REF_SHA	$SHA_TO_TEST	$(numactl --hardware | sed -n 's/^available:[[:space:]]\+/numactl-available	/p')
EOF

    # Also insert some data about the check into the CI checks table.
    "${client[@]}" --query "INSERT INTO "'"'"gh-data"'"'".checks FORMAT TSVWithNamesAndTypes" \
        < ci-checks.tsv

    set -x
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
    numactl --show ||:
    numactl --hardware ||:
    lscpu ||:
    dmidecode -t 4 ||:
    time restart
    ;&
"run_tests")
    # Ignore the errors to collect the log and build at least some report, anyway
    time run_tests ||:
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
    ( get_profiles || { restart && get_profiles ; } ) ||:

    # Kill the whole process group, because somehow when the subshell is killed,
    # the sleep inside remains alive and orphaned.
    while env kill -- -$watchdog_pid ; do sleep 1; done

    # Stop the servers to free memory for the subsequent query analysis.
    while pkill clickhouse-serv; do echo . ; sleep 1 ; done
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
"upload_results")
    time upload_results ||:
    ;&
esac

# Print some final debug info to help debug Weirdness, of which there is plenty.
jobs
pstree -apgT

