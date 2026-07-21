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
LEFT_SERVER_KEEPER_PORT=9181
LEFT_SERVER_KEEPER_RAFT_PORT=9234
LEFT_SERVER_INTERSERVER_PORT=9009
LEFT_SERVER_HTTP_PORT=8123
# patched version
RIGHT_SERVER_PORT=19001
RIGHT_SERVER_KEEPER_PORT=19181
RIGHT_SERVER_KEEPER_RAFT_PORT=19234
RIGHT_SERVER_INTERSERVER_PORT=19009
RIGHT_SERVER_HTTP_PORT=18123

# abort_conf   -- abort if some options is not recognized
# abort        -- abort if something is not right in the env (i.e. per-cpu arenas does not work)
# narenas      -- set them explicitly to avoid disabling per-cpu arena in env
#                 that returns different number of CPUs for some of the following
#                 _SC_NPROCESSORS_ONLN/_SC_NPROCESSORS_CONF/sched_getaffinity
export MALLOC_CONF="abort_conf:true,abort:true,narenas:$(nproc --all)"

# jemalloc allocation sampling rate (lg2 of the average byte interval between
# samples) for the per-query profiler used in the dedicated profile runs.
# Lower than the 512 KiB (19) default: we profile single queries in isolation,
# so we need a denser profile to get useful JemallocSample flamegraphs.
# Must match CHServer.JEMALLOC_PROFILER_SAMPLING_RATE in performance_tests.py.
JEMALLOC_PROFILER_SAMPLING_RATE=16

# Settings for the report-building clickhouse-local (post-processing of the perf
# results, not the measured servers). Keep in sync with
# REPORT_LOCAL_{QUERY,SERVER}_SETTINGS in performance_tests.py.
# Keep report aggregations in RAM: report/tmp cannot hold a spill of the
# heaviest randomization queries, so spilling only fails with NOT_ENOUGH_SPACE.
CHPC_REPORT_LOCAL_QUERY_SETTINGS="--max_bytes_before_external_group_by=0 --max_bytes_ratio_before_external_group_by=0 --max_bytes_before_external_sort=0 --max_bytes_ratio_before_external_sort=0"
# Track each process against its own RSS, not the job cgroup (MEMORY_LIMIT_EXCEEDED).
CHPC_REPORT_LOCAL_SERVER_SETTINGS="--memory_worker_use_cgroup=0"

function wait_for_server # port, pid
{
    for _ in {1..60}
    do
        if clickhouse-client --port "$1" --receive_timeout=5 --query "select 1" || ! kill -0 "$2"
        then
            break
        fi
        sleep 1
    done

    if ! clickhouse-client --port "$1" --receive_timeout=5 --query "select 1"
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
    # backups disk uses absolute path, and this overlaps between servers, that could lead to errors
    rm right/config/config.d/backups.xml ||:
    rm left/config/config.d/backups.xml ||:
    cp -rv right/config left ||:

    # Start a temporary server to rename the tables
    while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
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
        --keeper_server.storage_path coordination0
        --tcp_port $LEFT_SERVER_PORT
    )
    left/clickhouse-server "${setup_left_server_opts[@]}" &> setup-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid
    set +m

    wait_for_server $LEFT_SERVER_PORT $left_pid
    echo "Server for setup started"

    clickhouse-client --port $LEFT_SERVER_PORT --query "create database test" ||:
    clickhouse-client --port $LEFT_SERVER_PORT --query "rename table datasets.hits_v1 to test.hits" ||:

    while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
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
    cp -R coordination0 left/coordination

    cp -al db0/ right/db/
    cp -R coordination0 right/coordination

    # Symlink user_files from the repository into both servers' user_files directories
    if [ -d "$script_dir/../../../../tests/performance/user_files" ]; then
        for f in "$script_dir/../../../../tests/performance/user_files"/*; do
            [ -e "$f" ] || continue
            ln -sf "$(readlink -f "$f")" left/db/user_files/
            ln -sf "$(readlink -f "$f")" right/db/user_files/
        done
    fi
}

# addressToLine resolves a frame to "file:line" only where DWARF covers
# ClickHouse code. PR builds use -g0 (DISABLE_ALL_DEBUG_SYMBOLS): the symbol
# table remains (addressToSymbol works) but there is no line info, so the patched
# (right) binary symbolizes differently from the reference (left, master) build
# and flamegraph tooling cannot match the frames. A ".debug_info" section is not
# a reliable signal (Rust crates emit one even under -g0), so probe how many
# system.stack_trace frames resolve to a line on each binary and strip the
# reference only when the patched binary resolves far fewer. Merge-to-master
# resolves comparably on both and is left untouched.
function match_reference_debug_info
{
    local left right left_lines right_lines
    left=$(readlink -f left/clickhouse-server)
    right=$(readlink -f right/clickhouse-server)
    # Running clickhouse also decompresses the self-extracting binary in place.
    local probe="select countIf(addressToLine(arrayJoin(trace)) like '%:%') from system.stack_trace"
    left_lines=$("$left" local --allow_introspection_functions=1 --query "$probe" 2>/dev/null ||:)
    right_lines=$("$right" local --allow_introspection_functions=1 --query "$probe" 2>/dev/null ||:)
    if [ "$(( ${right_lines:-0} * 4 ))" -lt "${left_lines:-0}" ]; then
        strip --strip-debug "$left"
    fi
}

# On x86_64 the measured servers are pinned to one hyperthread per physical
# core (m7i.4xlarge: 8 physical cores x 2 HT), so that whether two query
# threads share a hyperthread sibling does not depend on the scheduler; the
# matching max_threads=8 users.d override is written at job setup. Prints a
# "taskset -c <list>" prefix on x86_64 and nothing on other architectures
# (arm has real cores only and is unchanged). Must stay in sync with
# get_physical_core_cpu_list in ci/jobs/performance_tests.py, which pins the
# initial server starts the same way.
function cpu_pinning_prefix
{
    if [ "$(uname -m)" != "x86_64" ]
    then
        return 0
    fi
    local cpus half
    # thread_siblings_list formats: "0,8", "0-1", "0" (no SMT). Keep the
    # first sibling of each unique pair.
    cpus=$(sed 's/[,-].*//' /sys/devices/system/cpu/cpu*/topology/thread_siblings_list 2>/dev/null \
        | sort -un | paste -sd, - ||:)
    if [ -z "$cpus" ]
    then
        # Fallback: the first half of the cpus (siblings are enumerated after
        # all physical cores on our runners).
        half=$(( $(nproc) / 2 ))
        if [ "$half" -lt 1 ]; then half=1; fi
        cpus="0-$(( half - 1 ))"
    fi
    echo "taskset -c $cpus"
}

function restart
{
    while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
    echo all killed

    match_reference_debug_info

    # Intentionally unquoted below: expands to nothing on non-x86_64.
    local pinning_prefix
    pinning_prefix=$(cpu_pinning_prefix)

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
        # The perf-comparison config removes <http_port>; re-enable it on the
        # command line (a documented config override) with a distinct port per
        # server, so that shell-script tests can talk to the server over HTTP.
        --http_port $LEFT_SERVER_HTTP_PORT
        --keeper_server.tcp_port $LEFT_SERVER_KEEPER_PORT
        --keeper_server.raft_configuration.server.port $LEFT_SERVER_KEEPER_RAFT_PORT
        --keeper_server.storage_path left/coordination
        --zookeeper.node.port $LEFT_SERVER_KEEPER_PORT
        --interserver_http_port $LEFT_SERVER_INTERSERVER_PORT
        --jemalloc_profiler_sampling_rate $JEMALLOC_PROFILER_SAMPLING_RATE
    )
    $pinning_prefix left/clickhouse-server "${left_server_opts[@]}" &>> left-server-log.log &
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
        --http_port $RIGHT_SERVER_HTTP_PORT
        --keeper_server.tcp_port $RIGHT_SERVER_KEEPER_PORT
        --keeper_server.raft_configuration.server.port $RIGHT_SERVER_KEEPER_RAFT_PORT
        --keeper_server.storage_path right/coordination
        --zookeeper.node.port $RIGHT_SERVER_KEEPER_PORT
        --interserver_http_port $RIGHT_SERVER_INTERSERVER_PORT
        --jemalloc_profiler_sampling_rate $JEMALLOC_PROFILER_SAMPLING_RATE
    )
    $pinning_prefix right/clickhouse-server "${right_server_opts[@]}" &>> right-server-log.log &
    right_pid=$!
    kill -0 $right_pid
    disown $right_pid

    set +m

    wait_for_server $LEFT_SERVER_PORT $left_pid
    echo left ok

    wait_for_server $RIGHT_SERVER_PORT $right_pid
    echo right ok

    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.tables where database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.build_options"
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.tables where database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
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
# TODO: remove?
#    elif [ "$PR_TO_TEST" == "0" ]
#    then
#        # When testing commits from master, use the older test files. This
#        # allows the tests to pass even when we add new functions and tests for
#        # them, that are not supported in the old revision.
#        test_prefix=left/performance
    else
        # For PRs, use newer test files so we can test these changes.
        test_prefix=right/performance
    fi

    run_only_changed_tests=0

    # Determine which tests to run.
    if [ -v CHPC_TEST_GREP ]
    then
        # Run only explicitly specified tests, if any.
        # shellcheck disable=SC2010
        test_files=($(ls "$test_prefix" | rg "$CHPC_TEST_GREP" | xargs -I{} -n1 readlink -f "$test_prefix/{}"))
# TODO: remove
#    elif [ "$PR_TO_TEST" -ne 0 ] \
#        && [ "$(wc -l < changed-test-definitions.txt)" -gt 0 ] \
#        && [ "$(wc -l < other-changed-files.txt)" -eq 0 ]
#    then
#        # If only the perf tests were changed in the PR, we will run only these
#        # tests. The lists of changed files are prepared in entrypoint.sh because
#        # it has the repository.
#        test_files=($(sed "s/tests\/performance/${test_prefix//\//\\/}/" changed-test-definitions.txt))
#        run_only_changed_tests=1
    else
        # The default -- run all tests found in the test dir.
        test_files=($(ls "$test_prefix"/*.xml))
    fi

    # We can filter out certain tests
    if [ -v CHPC_TEST_GREP_EXCLUDE ]; then
        # filter tests array in bash https://stackoverflow.com/a/40375567
        filtered_test_files=( $( for i in ${test_files[@]} ; do echo $i ; done | rg -v ${CHPC_TEST_GREP_EXCLUDE} ) )
        test_files=("${filtered_test_files[@]}")
    fi

    # We split perf tests into multiple checks to make them faster
    if [ -v CHPC_TEST_RUN_BY_HASH_TOTAL ]; then
        # filter tests array in bash https://stackoverflow.com/a/40375567
        for index in "${!test_files[@]}"; do
            [ $(( index % CHPC_TEST_RUN_BY_HASH_TOTAL )) != "$CHPC_TEST_RUN_BY_HASH_NUM" ] && \
                unset -v 'test_files[$index]'
        done
        # to have sequential indexes...
        test_files=("${test_files[@]}")
    fi

    if [ "$run_only_changed_tests" -ne 0 ]; then
        if [ ${#test_files[@]} -eq 0 ]; then
            time "$script_dir/report.py" --no-tests-run > report.html
            exit 0
        fi
    fi

    # For PRs w/o changes in test definitions, test only a subset of queries,
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
        clickhouse-client --port $LEFT_SERVER_PORT --receive_timeout=5 --query "select 1 format Null" \
            || { echo $test_name >> left-server-died.log ; restart ; }
        clickhouse-client --port $RIGHT_SERVER_PORT --receive_timeout=5 --query "select 1 format Null" \
            || { echo $test_name >> right-server-died.log ; restart ; }

        test_name=$(basename "$test" ".xml")
        echo test "$test_name"

        # Don't profile if we're past the time limit.
        # Use awk because bash doesn't support floating point arithmetic.
        profile_seconds=$(awk "BEGIN { print ($profile_seconds_left > 0 ? 10 : 0) }")

        max_queries=0
# TODO: remove?
#        if rg --quiet "$(basename $test)" changed-test-definitions.txt
#        then
#          # Run all queries from changed test files to ensure that all new queries will be tested.
#          max_queries=0
#        else
#          max_queries=$CHPC_MAX_QUERIES
#        fi

        (
            set +x
            argv=(
                --host localhost localhost
                --port "$LEFT_SERVER_PORT" "$RIGHT_SERVER_PORT"
                # Binary paths and HTTP ports are used by shell-script tests
                # (<query type="shell">), e.g. clickhouse-local startup and HTTP
                # reads. They are parallel to --host/--port (left, then right).
                --binary left/clickhouse right/clickhouse
                --http-port "$LEFT_SERVER_HTTP_PORT" "$RIGHT_SERVER_HTTP_PORT"
                --runs "$CHPC_RUNS"
                --max-queries "$max_queries"
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

    # Dump the query_log columns the report reads (query_id, ProfileEvents,
    # query_duration_ms, memory_usage, read_bytes, written_bytes) plus those
    # useful for manual debugging (query text, Settings, timings, row/byte
    # counts, errors). The other ~70 columns are dropped to keep the artifact
    # smaller.
    local query_log_columns="type, event_time, query_id, query_duration_ms, memory_usage, read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, query, exception_code, exception, ProfileEvents, Settings"

    clickhouse-client --port $LEFT_SERVER_PORT --query "select $query_log_columns from system.query_log where type in ('QueryFinish', 'ExceptionWhileProcessing') format TSVWithNamesAndTypes" > left-query-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.trace_log format TSVWithNamesAndTypes" > left-trace-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > left-addresses.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.metric_log format TSVWithNamesAndTypes" > left-metric-log.tsv ||: &
    clickhouse-client --port $LEFT_SERVER_PORT --query "select * from system.asynchronous_metric_log format TSVWithNamesAndTypes" > left-async-metric-log.tsv ||: &

    clickhouse-client --port $RIGHT_SERVER_PORT --query "select $query_log_columns from system.query_log where type in ('QueryFinish', 'ExceptionWhileProcessing') format TSVWithNamesAndTypes" > right-query-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.trace_log format TSVWithNamesAndTypes" > right-trace-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > right-addresses.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.metric_log format TSVWithNamesAndTypes" > right-metric-log.tsv ||: &
    clickhouse-client --port $RIGHT_SERVER_PORT --query "select * from system.asynchronous_metric_log format TSVWithNamesAndTypes" > right-async-metric-log.tsv ||: &

    wait

    # Just check that the servers are alive so that we return a proper exit code.
    # We don't consistently check the return codes of the above background jobs.
    clickhouse-client --port $LEFT_SERVER_PORT --receive_timeout=5 --query "select 1"
    clickhouse-client --port $RIGHT_SERVER_PORT --receive_timeout=5 --query "select 1"
}

# Build and analyze randomization distribution for all queries.
function analyze_queries
{
rm -v analyze-commands.txt analyze-errors.log all-queries.tsv unstable-queries.tsv ./*-report.tsv raw-queries.tsv ||:
rm -rf analyze ||:
mkdir analyze analyze/tmp ||:

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
    sed -n "s/^partial\t/$test_name\t/p" < "$test_file" >> "analyze/partial-queries.tsv"
done

# for each query run, prepare array of metrics from query log
clickhouse-local --query "
create view query_runs as select * from file('analyze/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

-- Separately process backward-incompatible ('partial') queries which we could only run on the new server
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
    from file('left-query-log.tsv', TSVWithNamesAndTypes);

create view right_query_log as select *
    from file('right-query-log.tsv', TSVWithNamesAndTypes);

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
        -- sumMapState with the list of all keys with nullable '0' values because sumMap removes keys with default values
        -- and 0::Nullable != NULL
        with (select groupUniqArrayArray(mapKeys(ProfileEvents)) from query_logs) as all_names
            select arrayReduce('sumMapState', [(all_names, arrayMap(x->0::Nullable(Float64), all_names))])
        ) as all_metrics
    -- Take version/query_id from query_runs, the preserved side of the RIGHT JOIN.
    -- For SQL queries the query_log always matches, so this is the same value;
    -- but shell-script queries (<query type="shell">) have no query_log row, and
    -- the unqualified columns would otherwise default to query_logs' 0/'' and
    -- collapse both servers' runs onto version 0.
    select test, query_index, query_runs.version version, query_runs.query_id query_id,
        (finalizeAggregation(
            arrayReduce('sumMapMergeState',
                [
                    all_metrics,
                    arrayReduce('sumMapState',
                        [(mapKeys(ProfileEvents),
                            arrayMap(x->toNullable(toFloat64(x)), mapValues(ProfileEvents)))]
                    ),
                    arrayReduce('sumMapState', [(
                        ['client_time', 'server_time', 'memory_usage'],
                        [toNullable(toFloat64(query_runs.time)), toNullable(toFloat64(query_duration_ms / 1000.)), toNullable(toFloat64(memory_usage))]
                      )])
                ]
            )) as metrics_tuple).1 metric_names,
        arrayMap(x->if(isNaN(x),0,x), metrics_tuple.2) metric_values
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
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2> >(tee -a analyze/errors.log 1>&2)

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
    rg "^$prefix	" "analyze/query-run-metrics-for-stats.tsv" > "$file" &
    printf "%s\0\n" \
        "clickhouse-local \
            --file \"$file\" \
            --structure 'test text, query text, run int, version UInt8, metrics Array(float)' \
            --query \"$(cat "$script_dir/eqmed.sql")\" \
            $CHPC_REPORT_LOCAL_QUERY_SETTINGS \
            -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS \
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
#numactl --show
#numactl --cpunodebind=all --membind=all numactl --show

# Notes for parallel:
#
# Some queries can consume 8+ GB of memory, so it worth to limit amount of jobs
# that can be run in parallel.
#
# --memfree:
#
#   will kill jobs, which is not good (and retried until --retries exceeded)
#
# --memsuspend:
#
#   If the available memory falls below 2 * size, GNU parallel will suspend some of the running jobs.
#
# With external aggregation disabled, a single heavy job stays in RAM, so bound
# concurrency by host RAM to avoid MEMORY_LIMIT_EXCEEDED; --memsuspend suspends
# some jobs anyway if they collectively approach the limit.
report_jobs=$(( $(awk '/MemTotal/ {print int($2/1024/1024)}' /proc/meminfo) / 15 ))
[ "$report_jobs" -lt 1 ] && report_jobs=1
parallel -v -j "$report_jobs" --joblog analyze/parallel-log.txt --memsuspend 15G --null < analyze/commands.txt 2>> analyze/errors.log

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
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2> >(tee -a analyze/errors.log 1>&2)
}

# Confirm the queries flagged as changed by rerunning them on freshly restarted
# servers. The changed_fail verdict from a single test pass is sensitive to
# one-off environmental noise (page cache state, CPU frequency, noisy
# neighbours), so before failing the check we rerun only the flagged queries
# after a full server restart and demote the changes that do not reproduce.
# Demoted queries stay visible in a separate 'Unconfirmed Changes' report table.
#
# This step is strictly advisory and FAIL-OPEN: on any error we log, leave
# analyze-confirm/unconfirmed-queries.tsv absent or empty (which report() reads
# as "demote nothing"), and return success, so a confirmation failure can never
# change today's verdicts or fail the job. For the same reason nothing here
# writes to analyze/errors.log or report/errors.log (a non-empty
# report/errors.log fails the check) -- confirmation errors go to the job log
# and analyze-confirm/errors.log only. The caller must invoke it as
# `confirm_changes ||:` (this also disables errexit inside, so partial failures
# fall through the explicit guards below instead of killing the script).
function confirm_changes
{
rm -rf analyze-confirm ||:
mkdir analyze-confirm analyze-confirm/tmp ||:

# report() joins the per-query thresholds from historical-thresholds.tsv,
# which may be absent in manual runs. Use a private empty copy then, instead
# of creating the file at the path report() reads (fail-open: do not alter
# the inputs of the main flow).
if [ -e historical-thresholds.tsv ]
then
    cp historical-thresholds.tsv analyze-confirm/historical-thresholds.tsv
else
    touch analyze-confirm/historical-thresholds.tsv
fi

# 1. Collect the (test, query_index) pairs flagged as changed_fail, with the
# same rule and thresholds as the 'queries' table in report(): client_time
# metric, per-query threshold = ceil(greatest(0.15, historical, per-test), 2),
# non-strict comparison with stat_threshold.
if ! clickhouse-local --query "
create view query_metric_stats as
    select * from file('analyze/query-metric-stats-denorm.tsv',
        TSVWithNamesAndTypes,
        'test text, query_index int, metric_name text, left float, right float,
            diff float, stat_threshold float')
    ;

create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create table flagged_queries engine File(TSV, 'analyze-confirm/flagged-queries.tsv')
    as select
        query_metric_stats.test test, query_metric_stats.query_index query_index,
        diff, stat_threshold,
        -- Carried along so the confirmation rerun is held to the same
        -- per-query bar as the flagging rule (not just the 0.15 floor):
        -- historically noisy queries have learned thresholds above the floor.
        ceil(greatest(0.15, historical_thresholds.max_diff,
            test_thresholds.report_threshold), 2) changed_threshold
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    left join file('analyze-confirm/historical-thresholds.tsv', TSV,
        'test text, query_index int, max_diff float, max_stat_threshold float,
            query_display_name text') historical_thresholds
        on query_metric_stats.test = historical_thresholds.test
            and query_metric_stats.query_index = historical_thresholds.query_index
            and query_display_names.query_display_name = historical_thresholds.query_display_name
    left join file('analyze/report-thresholds.tsv', TSV,
        'test text, report_threshold float') test_thresholds
        on query_metric_stats.test = test_thresholds.test
    where metric_name = 'client_time'
        and abs(diff) > ceil(greatest(0.15, historical_thresholds.max_diff,
            test_thresholds.report_threshold), 2)
        and abs(diff) >= stat_threshold
    order by test, query_index
    ;
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2>> analyze-confirm/errors.log
then
    echo "confirm_changes: failed to compute the flagged query list, skipping confirmation"
    return 0
fi

local flagged_count
flagged_count=$(wc -l < analyze-confirm/flagged-queries.tsv)
if [ "$flagged_count" -eq 0 ]
then
    echo "confirm_changes: no queries flagged as changed, nothing to confirm"
    return 0
fi
# Cap the total confirmation cost: a PR that flags very many queries (e.g. a
# global slowdown or a runaway environment) would double the test time, and
# such wholesale changes do not need per-query confirmation anyway.
if [ "$flagged_count" -gt 100 ]
then
    echo "confirm_changes: WARNING: $flagged_count flagged queries exceed the cap of 100, skipping confirmation"
    return 0
fi

# 2. Restart both servers so that the reruns measure a fresh process state
# (empty caches, unfragmented heap), reusing the main restart machinery.
if ! restart
then
    echo "confirm_changes: server restart failed, skipping confirmation"
    while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
    echo all killed
    return 0
fi

# The caller runs us with errexit disabled (fail-open), which also masks
# failures inside restart: its return value only reflects the last check.
# Verify both servers explicitly before spending the rerun budget.
if ! clickhouse-client --port "$LEFT_SERVER_PORT" --receive_timeout=5 \
        --query "select 1 format Null" \
    || ! clickhouse-client --port "$RIGHT_SERVER_PORT" --receive_timeout=5 \
        --query "select 1 format Null"
then
    echo "confirm_changes: servers not healthy after restart, skipping confirmation"
    while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
    echo all killed
    return 0
fi

# 3. Rerun each affected test limited to its flagged query indexes, writing
# raw results into analyze-confirm/ so the main *-raw.tsv files stay intact.
# Mirror run_tests' test file resolution; fall back to the in-repo tests for
# the CI flow where performance_tests.py runs the tests itself.
local test_prefix
if [ -v CHPC_TEST_PATH ]
then
    test_prefix="$CHPC_TEST_PATH"
elif [ -d right/performance ]
then
    test_prefix=right/performance
else
    test_prefix="$script_dir/../../../../tests/performance"
fi

local perf_py="$script_dir/perf.py"
if ! [ -e "$perf_py" ]
then
    perf_py="$script_dir/../../../../tests/performance/scripts/perf.py"
fi

# Also cap the confirmation wall-clock time. Tests not rerun keep their
# original verdict (fail-open in the strict direction: no demotion).
local confirm_deadline=$(( SECONDS + 1200 ))
local confirm_test confirm_test_file confirm_indexes
for confirm_test in $(cut -f1 analyze-confirm/flagged-queries.tsv | sort | uniq)
do
    if [ "$SECONDS" -ge "$confirm_deadline" ]
    then
        echo "confirm_changes: WARNING: time budget exhausted, queries of the remaining tests keep their original verdict"
        break
    fi

    confirm_test_file="$test_prefix/$confirm_test.xml"
    if ! [ -e "$confirm_test_file" ]
    then
        echo "confirm_changes: cannot find $confirm_test_file, its queries keep their original verdict"
        continue
    fi

    confirm_indexes=$(awk -F'\t' -v t="$confirm_test" '$1 == t { print $2 }' \
        analyze-confirm/flagged-queries.tsv | sort -n | uniq | tr '\n' ' ')

    # The test file goes first: argparse would swallow it into the greedy
    # nargs='*' --queries-to-run otherwise. No profiling on reruns.
    # shellcheck disable=SC2086
    if ! "$perf_py" "$confirm_test_file" \
        --host localhost localhost \
        --port "$LEFT_SERVER_PORT" "$RIGHT_SERVER_PORT" \
        --binary left/clickhouse right/clickhouse \
        --http-port "$LEFT_SERVER_HTTP_PORT" "$RIGHT_SERVER_HTTP_PORT" \
        --runs "${CHPC_RUNS:-7}" --max-queries 0 --profile-seconds 0 \
        --queries-to-run $confirm_indexes \
        > "analyze-confirm/$confirm_test-raw.tsv" \
        2> "analyze-confirm/$confirm_test-err.log"
    then
        echo "confirm_changes: rerun of $confirm_test failed, its queries keep their original verdict"
    fi
done

# Stop the servers again: the subsequent report stages run memory-heavy
# clickhouse-local queries and expect the servers to be down.
while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
echo all killed

# 4. Recompute the eqmed.sql statistics on the rerun data, exactly like
# analyze_queries does. The confirmation verdict is made on client_time (the
# same metric as changed_fail), which comes from the perf.py-reported run
# times, so the query logs are not needed and the metrics array has a single
# element.
touch analyze-confirm/query-runs.tsv
local raw_file rerun_test_name
for raw_file in analyze-confirm/*-raw.tsv
do
    [ -e "$raw_file" ] || continue
    rerun_test_name=$(basename "$raw_file" "-raw.tsv")
    sed -n "s/^query\t/$rerun_test_name\t/p" < "$raw_file" >> analyze-confirm/query-runs.tsv
done

if ! clickhouse-local --query "
create view query_runs as select * from file('analyze-confirm/query-runs.tsv', TSV,
    'test text, query_index int, query_id text, version UInt8, time float');

-- Same even-run-count filter as analyze_queries: a server death mid-test
-- leaves an odd number of runs, which would break the median split.
create view broken_queries as
    select test, query_index
    from query_runs
    group by test, query_index
    having count(*) % 2 != 0
    ;

create table query_run_metrics_for_stats engine File(
        TSV, 'analyze-confirm/query-run-metrics-for-stats.tsv')
    as select test, query_index, 0 run, version, [toFloat64(time)] metrics
    from query_runs
    where (test, query_index) not in broken_queries
    order by test, query_index, run, version
    ;
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2>> analyze-confirm/errors.log
then
    echo "confirm_changes: failed to prepare the rerun measurements, skipping confirmation"
    return 0
fi

# The same per-query eqmed.sql invocation path as in analyze_queries. A failed
# invocation only loses that query's rerun stats, so it keeps its original
# verdict.
touch analyze-confirm/commands.txt analyze-confirm/query-metric-stats.tsv
( set +x # do not bloat the log
IFS=$'\n'
for prefix in $(cut -f1,2 "analyze-confirm/query-run-metrics-for-stats.tsv" | sort | uniq)
do
    file="analyze-confirm/tmp/${prefix//	/_}.tsv"
    rg "^$prefix	" "analyze-confirm/query-run-metrics-for-stats.tsv" > "$file" &
    printf "%s\0\n" \
        "clickhouse-local \
            --file \"$file\" \
            --structure 'test text, query text, run int, version UInt8, metrics Array(float)' \
            --query \"$(cat "$script_dir/eqmed.sql")\" \
            $CHPC_REPORT_LOCAL_QUERY_SETTINGS \
            -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS \
            >> \"analyze-confirm/query-metric-stats.tsv\"" \
            2>> analyze-confirm/errors.log \
        >> analyze-confirm/commands.txt
done
wait
unset IFS
)

# The rerun arrays are single-metric and tiny, no need for the memory-bounded
# job count of the main analysis.
parallel -v -j "$(nproc --all)" --joblog analyze-confirm/parallel-log.txt --null \
    < analyze-confirm/commands.txt 2>> analyze-confirm/errors.log \
    || echo "confirm_changes: some rerun stats failed to compute, those queries keep their original verdict"

# 5. Demote the flagged queries whose rerun does not reproduce the change.
# Confirmed = same-direction diff clearing the exact production rule again:
# the per-query changed threshold carried from the flagging step (strict >)
# and the rerun's own noise threshold (non-strict >=). The inner join means
# only queries with rerun stats can be demoted: if the rerun failed or
# produced no stats, the original verdict stands (fail-open).
if ! clickhouse-local --query "
create view flagged_queries as select * from file('analyze-confirm/flagged-queries.tsv',
    TSV, 'test text, query_index int, diff float, stat_threshold float,
        changed_threshold float');

create view rerun_stats as
    select test, query_index,
        diff[1] diff_rerun, stat_threshold[1] stat_threshold_rerun
    from file('analyze-confirm/query-metric-stats.tsv', TSV,
        'left Array(float), right Array(float), diff Array(float),
            stat_threshold Array(float), test text, query_index int')
    ;

create table unconfirmed_queries engine File(TSV, 'analyze-confirm/unconfirmed-queries.tsv')
    as select flagged_queries.test test, flagged_queries.query_index query_index,
        diff_rerun, stat_threshold_rerun
    from flagged_queries
    join rerun_stats
        on flagged_queries.test = rerun_stats.test
            and flagged_queries.query_index = rerun_stats.query_index
    where not (sign(diff_rerun) = sign(diff)
        and abs(diff_rerun) > changed_threshold
        and abs(diff_rerun) >= stat_threshold_rerun)
    order by test, query_index
    ;
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2>> analyze-confirm/errors.log
then
    echo "confirm_changes: failed to compute the confirmation verdicts, skipping confirmation"
    rm -f analyze-confirm/unconfirmed-queries.tsv ||:
    return 0
fi

echo "confirm_changes: $(wc -l < analyze-confirm/unconfirmed-queries.tsv) of $flagged_count flagged queries did not reproduce after restart"
}

# Analyze results
function report
{
rm -r report ||:
mkdir report report/tmp ||:

rm ./*.{rep,svg} test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv all-queries.tsv run-errors.tsv ||:

cat analyze/errors.log >> report/errors.log ||:
cat profile-errors.log >> report/errors.log ||:

# The confirmation step (confirm_changes) is optional and fail-open: when it
# was skipped or failed, the demotion list is absent or empty, which must read
# as "demote nothing".
mkdir -p analyze-confirm ||:
touch analyze-confirm/unconfirmed-queries.tsv ||:

clickhouse-local --query "
create view query_display_names as select * from
    file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text')
    ;

create view partial_query_times as select * from
    file('analyze/partial-query-times.tsv', TSVWithNamesAndTypes,
        'test text, query_index int, time_stddev float, time_median double')
    ;

-- Report for backward-incompatible ('partial') queries that we could only run on the new server (e.g.
-- queries with new functions added in the tested PR).
create table partial_queries_report engine File(TSV, 'report/partial-queries-report.tsv')
    as select round(time_median, 3) time,
        round(time_stddev / time_median, 3) relative_time_stddev,
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

-- Queries flagged as changed whose difference did not reproduce when rerun
-- after a server restart (see confirm_changes). Empty unless the confirmation
-- step completed. They are demoted from changed_fail below, but stay visible
-- in the 'Unconfirmed Changes' table so that nothing silently disappears.
create view unconfirmed_queries as
    select * from file('analyze-confirm/unconfirmed-queries.tsv', TSV,
        'test text, query_index int, diff_rerun float, stat_threshold_rerun float')
    ;

create table report_thresholds engine File(TSVWithNamesAndTypes, 'report/thresholds.tsv')
    as select
        query_display_names.test test, query_display_names.query_index query_index,
        -- The floors (0.15 for 'changed', 0.25 for 'unstable') are the minimum
        -- relative difference we treat as a real change. They are intentionally
        -- above the level of run-to-run noise observed on CI runners: micro
        -- benchmarks routinely swing by 10-15% between two binaries because of
        -- machine noise (noisy neighbours, frequency scaling) and code-layout
        -- artifacts (function alignment/inlining shifts) unrelated to the tested
        -- change. For historically noisier queries the learned thresholds
        -- (1.5 * historical p99) are larger and take over.
        ceil(greatest(0.15, historical_thresholds.max_diff,
            test_thresholds.report_threshold), 2) changed_threshold,
        ceil(greatest(0.25, historical_thresholds.max_stat_threshold,
            test_thresholds.report_threshold + 0.1), 2) unstable_threshold,
        query_display_names.query_display_name query_display_name
    from query_display_names
    left join file('./historical-thresholds.tsv', TSV,
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
        -- A change is demoted from changed_fail (but not from changed_show, so
        -- it stays visible) when the confirmation rerun after a server restart
        -- did not reproduce it.
        abs(diff) > changed_threshold        and abs(diff) >= stat_threshold
            and ((query_metric_stats.test, query_metric_stats.query_index) not in
                (select test, query_index from unconfirmed_queries)) as changed_fail,
        abs(diff) > changed_threshold - 0.05 and abs(diff) >= stat_threshold as changed_show,

        -- Demoted queries are excluded here too: a demotion must not resurface
        -- as an 'unstable' failure through the flipped changed_fail.
        not changed_fail and stat_threshold > unstable_threshold
            and ((query_metric_stats.test, query_metric_stats.query_index) not in
                (select test, query_index from unconfirmed_queries)) as unstable_fail,
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
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        round(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select
        round(left, 3), round(right, 3), times_change_str,
        round(diff, 3), round(stat_threshold, 3),
        changed_fail, test, query_index, query_display_name
    from queries where changed_show order by abs(diff) desc;

-- Flagged changes that did not reproduce on the confirmation rerun. They no
-- longer fail the check (changed_fail was demoted above), but are reported
-- with both the original and the rerun statistics so demotions are auditable.
create table unconfirmed_changes_report engine File(TSV, 'report/unconfirmed-changes.tsv')
    as select
        round(left, 3), round(right, 3),
        round(diff, 3), round(stat_threshold, 3),
        round(diff_rerun, 3), round(stat_threshold_rerun, 3),
        queries.test test, queries.query_index query_index, query_display_name
    from queries
    join unconfirmed_queries
        on queries.test = unconfirmed_queries.test
            and queries.query_index = unconfirmed_queries.query_index
    order by abs(diff) desc;

create table unstable_queries_report engine File(TSV, 'report/unstable-queries.tsv')
    as select
        round(left, 3), round(right, 3), round(diff, 3),
        round(stat_threshold, 3), unstable_fail, test, query_index, query_display_name
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
    as with
        (times_speedup >= 1
            ? '-' || toString(round(times_speedup, 3)) || 'x'
            : '+' || toString(round(1 / times_speedup, 3)) || 'x')
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

create table wall_clock_time_per_test engine Memory as select *
    from file('wall-clock-times.tsv', TSV, 'test text, real float');

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
--  1) backward-incompatible ('partial') queries which run only on one server
--  3) some errors that make query run for a different number of times on a
--     particular server.
--
create view test_runs as
    select test,
        -- Default to 7 runs if we can't determine the number of runs.
        if((ceil(median(t.runs), 0) as r) != 0, r, 7) runs
    from (
        select
            -- The query id is the same for both servers, so no need to divide here.
            uniqExact(query_id) runs,
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
    as select
        test,
        round(real, 3),
        round(total_client_time, 3),
        queries,
        round(query_max, 3),
        round(avg_real_per_query, 3),
        round(query_min, 3),
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
    as with
        -- server_time is sometimes reported as zero (if it's less than 1 ms),
        -- so we have to work around this to not get an error about conversion
        -- of NaN to decimal.
        (left > right ? left / right : right / left) as times_change_float,
        isFinite(times_change_float) as times_change_finite,
        round(times_change_finite ? times_change_float : 1., 3) as times_change_decimal,
        times_change_finite
            ? (left > right ? '-' : '+') || toString(times_change_decimal) || 'x'
            : '--' as times_change_str
    select changed_fail, unstable_fail,
        round(left, 3), round(right, 3), times_change_str,
        round(isFinite(diff) ? diff : 0, 3),
        round(isFinite(stat_threshold) ? stat_threshold : 0, 3),
        test, query_index, query_display_name
    from queries order by test, query_index;


--------------------------------------------------------------------------------
-- various compatibility data formats follow, not related to the main report

-- keep the table in old format so that we can analyze new and old data together
create table queries_old_format engine File(TSVWithNamesAndTypes, 'queries.rep')
    as select 0 short, changed_fail, unstable_fail, left, right, diff,
        stat_threshold, test, query_display_name query
    from queries
    ;

-- new report for all queries with all metrics (no page yet)
-- The trailing changed_threshold/unstable_threshold columns are the per-query
-- thresholds computed in report_thresholds above (the 0.15/0.25 floors raised
-- by historical and per-test thresholds). They are exported so downstream
-- consumers (e.g. .claude/tools/fetch_perf_report.py) can classify queries
-- with the same effective thresholds as the CI gate instead of only the floor
-- constants. They are appended at the end to keep the existing column
-- positions stable for older consumers.
create table all_query_metrics_tsv engine File(TSV, 'report/all-query-metrics.tsv') as
    select metric_name, left, right, diff,
        floor(left > right ? left / right : right / left, 3),
        stat_threshold,
        query_metric_stats.test test, query_metric_stats.query_index query_index,
        query_display_names.query_display_name query_display_name,
        report_thresholds.changed_threshold changed_threshold,
        report_thresholds.unstable_threshold unstable_threshold
    from query_metric_stats
    left join query_display_names
        on query_metric_stats.test = query_display_names.test
            and query_metric_stats.query_index = query_display_names.query_index
    left join report_thresholds
        on query_display_names.test = report_thresholds.test
            and query_display_names.query_index = report_thresholds.query_index
            and query_display_names.query_display_name = report_thresholds.query_display_name
    order by test, query_index;
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2> >(tee -a report/errors.log 1>&2)

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
    from file('$version-query-log.tsv', TSVWithNamesAndTypes);

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
    from file('$version-trace-log.tsv', TSVWithNamesAndTypes);

create view addresses_src as
    -- Keep only the demangled symbol, dropping the 'file:line#'/'clickhouse#'
    -- prefix. PR builds use -g0 (DISABLE_ALL_DEBUG_SYMBOLS), so addressToLine has
    -- no DWARF and the prefix degrades to the binary basename ('clickhouse#...'),
    -- while master keeps 'file:line#'. A symbol-only name is identical on both,
    -- so per-side and differential flamegraphs stay comparable. A name has at most
    -- one '#' (demangled C++ symbols contain none). The clone.S filter runs first
    -- because it matches on the file part: the symbol itself ('__clone'/'clone')
    -- varies between builds, while dozens of unrelated symbols contain 'clone'.
    --
    -- Also drop the '.llvm.<hash>' suffix LLVM appends to internalized local
    -- symbols under LTO: the hash differs between builds, so it would split the
    -- same function into two frames.
    select addr, replaceRegexpOne(splitByChar('#', name)[-1], '[.]llvm[.][0-9]+', '') name
    from (
        select addr,
            -- Some functions change name between builds, e.g. '__clone' or 'clone'
            -- or even '__GI__clone@@GLIBC_2.32'. This breaks differential flame
            -- graphs, so filter them out here.
            [name, 'clone.S (filtered by script)', 'pthread_cond_timedwait (filtered by script)']
                -- this line is a subscript operator of the above array
                [1 + multiSearchFirstIndex(name, ['clone.S', 'pthread_cond_timedwait'])] name
        from file('$version-addresses.tsv', TSVWithNamesAndTypes)
    );

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
        -- Allocation samples are weighted by bytes; CPU/Real samples by count.
        multiIf(trace_type in ('MemorySample', 'JemallocSample'), toUInt64(sum(size)), count()) c
    from trace_log
    join unstable_query_runs using query_id
    -- Drop deallocation samples: their stack is the free site, not the
    -- allocation site, so they cannot be folded with the matching allocation.
    where size >= 0
    group by test, query_index, trace_type, trace
    order by test, query_index, trace_type, trace
    ;
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2> >(tee -a report/errors.log 1>&2) &
done
wait

# Allocation profiles (MemorySample/JemallocSample) fold byte totals rather
# than sample counts, so flamegraph.pl must label and color them as memory.
function flamegraph_opts # trace_type
{
    case "$1" in
        MemorySample | JemallocSample)
            echo "--countname=bytes --color=mem"
            ;;
    esac
}

# Create per-query flamegraphs
touch report/query-files.txt
IFS=$'\n'
for version in {right,left}
do
    for query in $(cut -d'	' -f1-4 "report/stacks.$version.tsv" | sort | uniq)
    do
        query_file=$(echo "$query" | cut -c-120 | sed 's/[/	]/_/g')
        trace_type=$(echo "$query" | cut -d'	' -f3)
        printf '%s\t%s\n' "$query_file" "$trace_type" >> report/query-files.txt

        # Allocation traces (MemorySample/JemallocSample) are sparse, so a query
        # may have samples on only one side. difffolded.pl below still needs both
        # inputs, so make the missing side an empty folded file.
        touch "report/tmp/$query_file.stacks.left.tsv" "report/tmp/$query_file.stacks.right.tsv"

        # Build separate .svg flamegraph for each query.
        # -F is somewhat unsafe because it might match not the beginning of the
        # string, but this is unlikely and escaping the query for grep is a pain.
        rg -F "$query	" "report/stacks.$version.tsv" \
            | cut -f 5- \
            | sed 's/\t/ /g' \
            | tee "report/tmp/$query_file.stacks.$version.tsv" \
            | flamegraph.pl --hash $(flamegraph_opts "$trace_type") > "$query_file.$version.svg" &
    done
done
wait
unset IFS

# Create differential flamegraphs. Frames are symbol-only (addresses_src strips
# the file:line/clickhouse prefix), so the two sides line up despite the PR side
# lacking DWARF.
while IFS=$'\t' read -r query_file trace_type
do
    difffolded.pl "report/tmp/$query_file.stacks.left.tsv" \
            "report/tmp/$query_file.stacks.right.tsv" \
        | tee "report/tmp/$query_file.stacks.diff.tsv" \
        | flamegraph.pl $(flamegraph_opts "$trace_type") > "$query_file.diff.svg" &
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
        rg -F "$query	" "report/metric-deviation.$version.tsv" \
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
        rg --no-filename --max-count=2 -i '\(Exception\|Error\):[^:]' "$log" \
            || rg --no-filename --max-count=2 -i '^[^ ]\+: ' "$log" \
            || head -10 "$log"
    } | sed "s/^/$test\t/" >> run-errors.tsv ||:
done

# Shell-script queries (<query type="shell">) report a failure on a server by
# emitting a `run-error` line to stdout (the per-test *-err.log is not always
# available at report time). Fold those into run-errors.tsv as well, in the
# 'test<tab>error' shape the Run Errors table and CIDB expect, so a shell test
# that failed on one server is reported instead of silently disappearing.
for test_file in *-raw.tsv
do
    test_name=$(basename "$test_file" "-raw.tsv")
    sed -n "s/^run-error\t\([0-9]*\)\t\([0-9]*\)\t/$test_name\tquery \1 server \2: /p" \
        < "$test_file" >> run-errors.tsv ||:
done
}

function report_metrics
{
rm -rf metrics ||:
mkdir metrics

clickhouse-local --query "
create view right_async_metric_log as
    select * from file('right-async-metric-log.tsv', TSVWithNamesAndTypes)
    ;

-- Use the right log as time reference because it may have higher precision.
create table metrics engine File(TSV, 'metrics/metrics.tsv') as
    with (select min(event_time) from right_async_metric_log) as min_time
    select metric, r.event_time - min_time event_time, l.value as left, r.value as right
    from right_async_metric_log r
    asof join file('left-async-metric-log.tsv', TSVWithNamesAndTypes) l
    on l.metric = r.metric and r.event_time <= l.event_time
    order by metric, event_time
    ;

-- Show metrics that have changed
create table changes engine File(TSV, 'metrics/changes.tsv')
    as select metric, left, right,
        round(diff, 3), round(times_diff, 3)
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
" $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS 2> >(tee -a metrics/errors.log 1>&2)

IFS=$'\n'
for prefix in $(cut -f1 "metrics/metrics.tsv" | sort | uniq)
do
    file="metrics/$prefix.tsv"
    rg "^$prefix	" "metrics/metrics.tsv" | cut -f2- > "$file"

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
    rm -f ci-checks.tsv

    clickhouse-local --query "
create view queries as select * from file('report/queries.tsv', TSVWithNamesAndTypes);

create table ci_checks engine File(TSVWithNamesAndTypes, 'ci-checks.tsv')
    as select
        $PR_TO_TEST :: UInt32 AS pull_request_number,
        '$SHA_TO_TEST' :: LowCardinality(String) AS commit_sha,
        '${CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME:-Performance}' :: LowCardinality(String) AS check_name,
        '$(sed -n 's/.*<!--status: \(.*\)-->/\1/p' report.html)' :: LowCardinality(String) AS check_status,
        (($(date +%s) - $CHPC_CHECK_START_TIMESTAMP) * 1000) :: UInt64 AS check_duration_ms,
        fromUnixTimestamp($CHPC_CHECK_START_TIMESTAMP) check_start_time,
        test_name :: LowCardinality(String) AS test_name ,
        test_status :: LowCardinality(String) AS test_status,
        test_duration_ms :: Float64 AS test_duration_ms,
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
            'https://s3.amazonaws.com/clickhouse-test-reports/$PR_TO_TEST/$SHA_TO_TEST/${CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME_PREFIX}/report.html#fail1' report_url
        union all
            select
                test || ' #' || toString(query_index) || '::' || test_desc_.1 test_name,
                multiIf(
                    changed_fail != 0 and diff > 0, 'slower',
                    unstable_fail != 0, 'unstable',
                    'success'
                ) test_status,
                test_desc_.2*1e3 test_duration_ms,
                'https://s3.amazonaws.com/clickhouse-test-reports/$PR_TO_TEST/$SHA_TO_TEST/${CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME_PREFIX}/'
                    || multiIf(
                        changed_fail != 0 and diff > 0, 'report.html#changes-in-performance.',
                        unstable_fail != 0, 'report.html#unstable-queries.',
                        'report.html#all-queries.'
                    )
                    || test || '.' || toString(query_index) report_url
            from queries
            array join map('old', left, 'new', right) as test_desc_
    )
;
    " $CHPC_REPORT_LOCAL_QUERY_SETTINGS -- $CHPC_REPORT_LOCAL_SERVER_SETTINGS

    # Upload some run attributes. I use this weird form because it is the same
    # form that can be used for historical data when you only have compare.log.
#    cat compare.log \
#        | sed -n '
#            s/.*Model name:[[:space:]]\+\(.*\)$/metric	lscpu-model-name	\1/p;
#            s/.*L1d cache:[[:space:]]\+\(.*\)$/metric	lscpu-l1d-cache	\1/p;
#            s/.*L1i cache:[[:space:]]\+\(.*\)$/metric	lscpu-l1i-cache	\1/p;
#            s/.*L2 cache:[[:space:]]\+\(.*\)$/metric	lscpu-l2-cache	\1/p;
#            s/.*L3 cache:[[:space:]]\+\(.*\)$/metric	lscpu-l3-cache	\1/p;
#            s/.*left_sha=\(.*\)$/old-sha	\1/p;
#            s/.*right_sha=\(.*\)/new-sha	\1/p' \
#        | awk '
#            BEGIN { FS = "\t"; OFS = "\t" }
#            /^old-sha/ { old_sha=$2 }
#            /^new-sha/ { new_sha=$2 }
#            /^metric/ { print old_sha, new_sha, $2, $3 }' \
#        | "${client[@]}" --query "INSERT INTO run_attributes_v1 FORMAT TSV"

    # Grepping numactl results from log is too crazy, I'll just call it again.
#    "${client[@]}" --query "INSERT INTO run_attributes_v1 FORMAT TSV" <<EOF
#$REF_SHA	$SHA_TO_TEST	$(numactl --show | sed -n 's/^cpubind:[[:space:]]\+/numactl-cpubind	/p')
#$REF_SHA	$SHA_TO_TEST	$(numactl --hardware | sed -n 's/^available:[[:space:]]\+/numactl-available	/p')
#EOF
#
#    # Also insert some data about the check into the CI checks table.
#    "${client[@]}" --query "INSERT INTO "'"'"default"'"'".checks FORMAT TSVWithNamesAndTypes" \
#        < ci-checks.tsv

#    set -x
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
# TODO: remove
#    numactl --show ||:
#    numactl --hardware ||:
#    lscpu ||:
#    dmidecode -t 4 ||:
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
    # TODO: while hangs
    #while env kill -- -$watchdog_pid ; do sleep 1; done
    env kill -- -$watchdog_pid

    # Stop the servers to free memory for the subsequent query analysis.
    while pkill -f clickhouse-serv ; do echo . ; sleep 1 ; done
    echo Servers stopped.
    ;&
"analyze_queries")
    time analyze_queries ||:
    ;&
"confirm_changes")
    # Rerun the changed queries on freshly restarted servers and demote the
    # changes that do not reproduce. Advisory and fail-open: `||:` both keeps
    # a confirmation failure from failing the job and disables errexit inside,
    # so the function's explicit guards decide what to skip.
    time confirm_changes ||:
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

# A non-empty report/errors.log fails the check. Print it so the cause is in
# the job log instead of only the results archive.
if [ -s report/errors.log ]; then
    echo "### report/errors.log ###"
    cat report/errors.log
fi

# Print some final debug info to help debug Weirdness, of which there is plenty.
jobs
#pstree -apgT
