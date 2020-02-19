#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap "kill 0" EXIT

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

mkdir db0 ||:

left_pr=$1
left_sha=$2

right_pr=$3
right_sha=$4

function download
{
    rm -r left ||:
    mkdir left ||:
    rm -r right ||:
    mkdir right ||:

    # might have the same version on left and right
    if ! [ "$left_sha" = "$right_sha" ]
    then
        wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$left_pr/$left_sha/performance/performance.tgz" -O- | tar -C left --strip-components=1 -zxv  &
        wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$right_pr/$right_sha/performance/performance.tgz" -O- | tar -C right --strip-components=1 -zxv &
    else
        wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$left_pr/$left_sha/performance/performance.tgz" -O- | tar -C left --strip-components=1 -zxv && cp -al left right
    fi

    cd db0 && wget -nv -nd -c "https://s3.mds.yandex.net/clickhouse-private-datasets/hits_10m_single/partitions/hits_10m_single.tar" -O- | tar -xv &
    cd db0 && wget -nv -nd -c "https://s3.mds.yandex.net/clickhouse-private-datasets/hits_100m_single/partitions/hits_100m_single.tar" -O- | tar -xv &
    cd db0 && wget -nv -nd -c "https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_v1.tar" -O- | tar -xv &
    cd db0 && wget -nv -nd -c "https://clickhouse-datasets.s3.yandex.net/values_with_expressions/partitions/test_values.tar" -O- | tar -xv &
    wait
}

function configure
{
    sed -i 's/<tcp_port>9000/<tcp_port>9001/g' left/config/config.xml
    sed -i 's/<tcp_port>9000/<tcp_port>9002/g' right/config/config.xml

    mkdir right/config/users.d ||:
    mkdir left/config/users.d ||:

    cat > right/config/config.d/zz-perf-test-tweaks-config.xml <<EOF
    <yandex>
        <logger>
            <console>true</console>
        </logger>
        <text_log remove="remove">
            <table remove="remove"/>
        </text_log>
        <metric_log remove="remove">
            <table remove="remove"/>
        </metric_log>
    </yandex>
EOF

    cat > right/config/users.d/zz-perf-test-tweaks-users.xml <<EOF
    <yandex>
        <profiles>
            <default>
                <query_profiler_real_time_period_ns>10000000</query_profiler_real_time_period_ns>
                <query_profiler_cpu_time_period_ns>0</query_profiler_cpu_time_period_ns>
                <allow_introspection_functions>1</allow_introspection_functions>
                <log_queries>1</log_queries>
            </default>
        </profiles>
    </yandex>
EOF

    cp right/config/config.d/zz-perf-test-tweaks-config.xml left/config/config.d/zz-perf-test-tweaks-config.xml
    cp right/config/users.d/zz-perf-test-tweaks-users.xml left/config/users.d/zz-perf-test-tweaks-users.xml

    rm left/config/config.d/metric_log.xml ||:
    rm left/config/config.d/text_log.xml ||:
    rm right/config/config.d/metric_log.xml ||:
    rm right/config/config.d/text_log.xml ||:

    # Start a temporary server to rename the tables
    while killall clickhouse ; do echo . ; sleep 1 ; done
    echo all killed

    set -m # Spawn temporary in its own process groups
    left/clickhouse server --config-file=left/config/config.xml -- --path db0 &> setup-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid
    set +m
    while ! left/clickhouse client --port 9001 --query "select 1" ; do kill -0 $left_pid ; echo . ; sleep 1 ; done
    echo server for setup started

    left/clickhouse client --port 9001 --query "create database test" ||:
    left/clickhouse client --port 9001 --query "rename table datasets.hits_v1 to test.hits" ||:

    while killall clickhouse ; do echo . ; sleep 1 ; done
    echo all killed

    # Remove logs etc, because they will be updated, and sharing them between
    # servers with hardlink might cause unpredictable behavior.
    rm db0/data/system/* -rf ||:
}

function restart
{
    while killall clickhouse ; do echo . ; sleep 1 ; done
    echo all killed

    # Make copies of the original db for both servers. Use hardlinks instead
    # of copying.
    rm -r left/db ||:
    rm -r right/db ||:
    cp -al db0/ left/db/
    cp -al db0/ right/db/

    set -m # Spawn servers in their own process groups

    left/clickhouse server --config-file=left/config/config.xml -- --path left/db &> left-server-log.log &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid

    right/clickhouse server --config-file=right/config/config.xml -- --path right/db &> right-server-log.log &
    right_pid=$!
    kill -0 $right_pid
    disown $right_pid

    set +m

    while ! left/clickhouse client --port 9001 --query "select 1" ; do kill -0 $left_pid ; echo . ; sleep 1 ; done
    echo left ok
    while ! right/clickhouse client --port 9002 --query "select 1" ; do kill -0 $right_pid ; echo . ; sleep 1 ; done
    echo right ok

    left/clickhouse client --port 9001 --query "select * from system.tables where database != 'system'"
    right/clickhouse client --port 9002 --query "select * from system.tables where database != 'system'"
}

function run_tests
{
    # Just check that the script runs at all
    "$script_dir/perf.py" --help > /dev/null

    rm -v test-times.tsv ||:

    # Why the ugly cut:
    # 1) can't make --out-format='%n' work for deleted files, it outputs things
    # like "deleted 1.xml";
    # 2) the output is not tab separated, but at least it's fixed width, so I
    # cut by characters.
    changed_files=$(rsync --dry-run --dirs --checksum --delete --itemize-changes left/performance/ right/performance/ | cut -c13-)

    # FIXME remove some broken long tests
    rm right/performance/{IPv4,IPv6,modulo,parse_engine_file,number_formatting_formats,select_format}.xml ||:

    test_files=$(ls right/performance/*)

    # FIXME a quick crutch to bring the run time down for the flappy tests --
    # run only those that have changed. Only on my prs for now.
    if grep Kuzmenkov right-commit.txt && [ "PR_TO_TEST" != "0" ]
    then
        test_files_override=$(cd right/performance && readlink -e $changed_files)
        if [ "test_files_override" != "" ]
        then
            test_files=$test_files_override
        fi
    fi

    # Run only explicitly specified tests, if any
    if [ -v CHPC_TEST_GLOB ]
    then
        test_files=$(ls right/performance/${CHPC_TEST_GLOB}.xml)
    fi

    # Run the tests
    for test in $test_files
    do
        test_name=$(basename $test ".xml")
        echo test $test_name

        TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
        # the grep is to filter out set -x output and keep only time output
        { time "$script_dir/perf.py" "$test" > "$test_name-raw.tsv" 2> "$test_name-err.log" ; } 2>&1 >/dev/null | grep -v ^+ >> "wall-clock-times.tsv" || continue

        grep ^query "$test_name-raw.tsv" | cut -f2- > "$test_name-queries.tsv"
        grep ^client-time "$test_name-raw.tsv" | cut -f2- > "$test_name-client-time.tsv"
        # this may be slow, run it in background
        right/clickhouse local --file "$test_name-queries.tsv" --structure 'query text, run int, version UInt32, time float' --query "$(cat $script_dir/eqmed.sql)" > "$test_name-report.tsv" &

        # Check that both servers are alive, to fail faster if they die.
        left/clickhouse client --port 9001 --query "select 1 format Null"
        right/clickhouse client --port 9002 --query "select 1 format Null"
    done

    unset TIMEFORMAT

    wait
}

function get_profiles
{
    # Collect the profiles
    left/clickhouse client --port 9001 --query "set query_profiler_cpu_time_period_ns = 0"
    left/clickhouse client --port 9001 --query "set query_profiler_real_time_period_ns = 0"
    right/clickhouse client --port 9001 --query "set query_profiler_cpu_time_period_ns = 0"
    right/clickhouse client --port 9001 --query "set query_profiler_real_time_period_ns = 0"

    left/clickhouse client --port 9001 --query "select * from system.query_log where type = 2 format TSVWithNamesAndTypes" > left-query-log.tsv ||: &
    left/clickhouse client --port 9001 --query "select * from system.trace_log format TSVWithNamesAndTypes" > left-trace-log.tsv ||: &
    left/clickhouse client --port 9001 --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > left-addresses.tsv ||: &

    right/clickhouse client --port 9002 --query "select * from system.query_log where type = 2 format TSVWithNamesAndTypes" > right-query-log.tsv ||: &
    right/clickhouse client --port 9002 --query "select * from system.trace_log format TSVWithNamesAndTypes" > right-trace-log.tsv ||: &
    right/clickhouse client --port 9002 --query "select arrayJoin(trace) addr, concat(splitByChar('/', addressToLine(addr))[-1], '#', demangle(addressToSymbol(addr)) ) name from system.trace_log group by addr format TSVWithNamesAndTypes" > right-addresses.tsv ||: &

    wait
}

# Analyze results
function report
{

for x in *.tsv
do
    # FIXME This loop builds column definitons from TSVWithNamesAndTypes in an
    # absolutely atrocious way. This should be done by the file() function itself.
    paste -d' ' \
        <(sed -n '1s/\t/\n/gp' "$x" | sed 's/\(^.*$\)/"\1"/') \
        <(sed -n '2s/\t/\n/gp' "$x" ) \
        | tr '\n' ', ' | sed 's/,$//' > "$x.columns"
done

rm *.rep test-times.tsv test-dump.tsv unstable.tsv unstable-query-ids.tsv unstable-query-metrics.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv ||:

right/clickhouse local --query "
create table queries engine Memory as select
        replaceAll(_file, '-report.tsv', '') test,
        left + right < 0.01 as short,
        -- FIXME Comparison mode doesn't make sense for queries that complete
        -- immediately, so for now we pretend they don't exist. We don't want to
        -- remove them altogether because we want to be able to detect regressions,
        -- but the right way to do this is not yet clear.
        not short and abs(diff) < 0.05 and rd[3] > 0.05 as unstable,
        not short and abs(diff) > 0.10 and abs(diff) > rd[3] as changed,
        *
    from file('*-report.tsv', TSV, 'left float, right float, diff float, rd Array(float), query text');

create table changed_perf_tsv engine File(TSV, 'changed-perf.tsv') as
    select left, right, diff, rd, test, query from queries where changed
    order by rd[3] desc;

create table unstable_queries_tsv engine File(TSV, 'unstable-queries.tsv') as
    select left, right, diff, rd, test, query from queries where unstable
    order by rd[3] desc;

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

create table all_queries_tsv engine File(TSV, 'all-queries.tsv') as
    select left, right, diff, rd, test, query
    from queries order by rd[3] desc;

create view right_query_log as select *
    from file('right-query-log.tsv', TSVWithNamesAndTypes, '$(cat right-query-log.tsv.columns)');

create view right_trace_log as select *
    from file('right-trace-log.tsv', TSVWithNamesAndTypes, '$(cat right-trace-log.tsv.columns)');

create view right_addresses as select *
    from file('right-addresses.tsv', TSVWithNamesAndTypes, '$(cat right-addresses.tsv.columns)');

create table unstable_query_ids engine File(TSVWithNamesAndTypes, 'unstable-query-ids.rep') as
    select query_id from right_query_log
    join unstable_queries_tsv using query
    ;

create table unstable_query_metrics engine File(TSVWithNamesAndTypes, 'unstable-query-metrics.rep') as
    select ProfileEvents.Values value, ProfileEvents.Names metric, query_id, query
    from right_query_log array join ProfileEvents
    where query_id in (unstable_query_ids)
    ;

create table unstable_query_traces engine File(TSVWithNamesAndTypes, 'unstable-query-traces.rep') as
    select count() value, right_addresses.name metric,
        unstable_query_ids.query_id, any(right_query_log.query) query
    from unstable_query_ids
    join right_query_log on right_query_log.query_id = unstable_query_ids.query_id
    join right_trace_log on right_trace_log.query_id = unstable_query_ids.query_id
    join right_addresses on addr = arrayJoin(trace)
    group by unstable_query_ids.query_id, metric
    order by count() desc
    ;

create table metric_devation engine File(TSVWithNamesAndTypes, 'metric-deviation.rep') as
    select floor((q[3] - q[1])/q[2], 3) d,
        quantilesExact(0.05, 0.5, 0.95)(value) q, metric, query
    from (select * from unstable_query_metrics
        union all select * from unstable_query_traces)
    join queries using query
    group by query, metric
    having d > 0.5
    order by any(rd[3]) desc, d desc
    ;
"

# Remember that grep sets error code when nothing is found, hence the bayan
# operator
grep Exception:[^:] *-err.log > run-errors.log ||:

$script_dir/report.py > report.html
}

case "$stage" in
"")
    ;&
"download")
    time download
    ;&
"configure")
    time configure
    ;&
"restart")
    time restart
    ;&
"run_tests")
    time run_tests
    ;&
"get_profiles")
    time get_profiles
    ;&
"report")
    time report
    ;&
esac
