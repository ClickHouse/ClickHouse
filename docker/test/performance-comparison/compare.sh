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

    la="$left_pr-$left_sha.tgz"
    ra="$right_pr-$right_sha.tgz"

    # might have the same version on left and right
    if ! [ "$la" = "$ra" ]
    then
        wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$left_pr/$left_sha/performance/performance.tgz" -O "$la" && tar -C left --strip-components=1 -zxvf "$la" &
        wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$right_pr/$right_sha/performance/performance.tgz" -O "$ra" && tar -C right --strip-components=1 -zxvf "$ra" &
    else
        wget -nv -nd -c "https://clickhouse-builds.s3.yandex.net/$left_pr/$left_sha/performance/performance.tgz" -O "$la" && { tar -C left --strip-components=1 -zxvf "$la" & tar -C right --strip-components=1 -zxvf "$ra" & } &
    fi

    cd db0 && wget -nv -nd -c "https://s3.mds.yandex.net/clickhouse-private-datasets/hits_10m_single/partitions/hits_10m_single.tar" && tar -xvf hits_10m_single.tar &
    cd db0 && wget -nv -nd -c "https://s3.mds.yandex.net/clickhouse-private-datasets/hits_100m_single/partitions/hits_100m_single.tar" && tar -xvf hits_100m_single.tar &
    cd db0 && wget -nv -nd -c "https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_v1.tar" && tar -xvf hits_v1.tar &
    wait

}

function configure
{
    sed -i 's/<tcp_port>9000/<tcp_port>9001/g' left/config/config.xml
    sed -i 's/<tcp_port>9000/<tcp_port>9002/g' right/config/config.xml

    cat > right/config/config.d/zz-perf-test-tweaks.xml <<EOF
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

    cp right/config/config.d/zz-perf-test-tweaks.xml left/config/config.d/zz-perf-test-tweaks.xml

    rm left/config/config.d/metric_log.xml ||:
    rm left/config/config.d/text_log.xml ||:
    rm right/config/config.d/metric_log.xml ||:
    rm right/config/config.d/text_log.xml ||:

    # Start a temporary server to rename the tables
    while killall clickhouse ; do echo . ; sleep 1 ; done
    echo all killed

    set -m # Spawn temporary in its own process groups
    left/clickhouse server --config-file=left/config/config.xml -- --path db0 &> setup-log.txt &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid
    set +m
    while ! left/clickhouse client --port 9001 --query "select 1" ; do kill -0 $left_pid ; echo . ; sleep 1 ; done
    echo server for setup started

    left/clickhouse client --port 9001 --query "create database test" ||:
    left/clickhouse client --port 9001 --query "rename table datasets.hits_v1 to test.hits" ||:
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

    left/clickhouse server --config-file=left/config/config.xml -- --path left/db &> left/log.txt &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid

    right/clickhouse server --config-file=right/config/config.xml -- --path right/db &> right/log.txt &
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

    # FIXME remove some broken long tests
    rm right/performance/{IPv4,IPv6,modulo,parse_engine_file,number_formatting_formats,select_format}.xml ||:

    # Run the tests
    for test in right/performance/${CHPC_TEST_GLOB:-*.xml}
    do
        test_name=$(basename $test ".xml")
        echo test $test_name
        TIMEFORMAT=$(printf "$test_name\t%%3R\t%%3U\t%%3S\n")
        # the grep is to filter out set -x output and keep only time output
        { time "$script_dir/perf.py" "$test" > "$test_name-raw.tsv" 2> "$test_name-err.log" ; } 2>&1 >/dev/null | grep -v ^+ >> "wall-clock-times.tsv" || continue
        grep ^query "$test_name-raw.tsv" | cut -f2- > "$test_name-queries.tsv"
        grep ^client-time "$test_name-raw.tsv" | cut -f2- > "$test_name-client-time.tsv"
        right/clickhouse local --file "$test_name-queries.tsv" --structure 'query text, run int, version UInt32, time float' --query "$(cat $script_dir/eqmed.sql)" > "$test_name-report.tsv"
    done
}

# Analyze results
function report
{
result_structure="left float, right float, diff float, rd Array(float), query text"
rm test-times.tsv test-dump.tsv unstable.tsv changed-perf.tsv unstable-tests.tsv unstable-queries.tsv bad-tests.tsv slow-on-client.tsv all-queries.tsv ||:
right/clickhouse local --query "
create table queries engine Memory as select
        replaceAll(_file, '-report.tsv', '') test,
        if(abs(diff) < 0.05 and rd[3] > 0.05,      1, 0) unstable,
        if(abs(diff) > 0.05 and abs(diff) > rd[3], 1, 0) changed,
        *
    from file('*-report.tsv', TSV, 'left float, right float, diff float, rd Array(float), query text')
    -- FIXME Comparison mode doesn't make sense for queries that complete
    -- immediately, so for now we pretend they don't exist. We don't want to
    -- remove them altogether because we want to be able to detect regressions,
    -- but the right way to do this is not yet clear.
    where left + right > 0.01;

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
        max(client) query_max, min(client) query_min, count(*) queries
    from query_time
    -- for consistency, filter out everything we filtered out of queries table
    semi join queries using query
    group by test;

create table test_times_tsv engine File(TSV, 'test-times.tsv') as
    select wall_clock.test, real,
        floor(total_client_time, 3),
        queries,
        floor(query_max, 3),
        floor(real / queries, 3) avg_real_per_query,
        floor(query_min, 3)
    from test_time right join wall_clock using test
    order by query_max / query_min desc;

create table all_queries_tsv engine File(TSV, 'all-queries.tsv') as
    select left, right, diff, rd, test, query
    from queries order by rd[3] desc;
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
    download
    configure
    restart
    run_tests
    ;&
"report")
    report
    ;&
esac
