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
    wget -q -nd -c "https://clickhouse-builds.s3.yandex.net/$left_pr/$left_sha/performance/performance.tgz" -O "$la" && tar -C left --strip-components=1 -zxvf "$la" &
    wget -q -nd -c "https://clickhouse-builds.s3.yandex.net/$right_pr/$right_sha/performance/performance.tgz" -O "$ra" && tar -C right --strip-components=1 -zxvf "$ra" &
    cd db0 && wget -q -nd -c "https://s3.mds.yandex.net/clickhouse-private-datasets/hits_10m_single/partitions/hits_10m_single.tar" && tar -xvf hits_10m_single.tar &
    cd db0 && wget -q -nd -c "https://s3.mds.yandex.net/clickhouse-private-datasets/hits_100m_single/partitions/hits_100m_single.tar" && tar -xvf hits_100m_single.tar &
    cd db0 && wget -q -nd -c "https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_v1.tar" && tar -xvf hits_v1.tar &
    wait

    # Use hardlinks instead of copying
    cp -al db0/ left/db/
    cp -al db0/ right/db/
}
download

function configure
{
    sed -i 's/<tcp_port>9000/<tcp_port>9001/g' right/config/config.xml

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
}
configure

function restart
{
    while killall clickhouse ; do echo . ; sleep 1 ; done
    echo all killed

    # Spawn servers in their own process groups
    set -m

    left/clickhouse server --config-file=left/config/config.xml -- --path left/db &> left/log.txt &
    left_pid=$!
    kill -0 $left_pid
    disown $left_pid

    right/clickhouse server --config-file=right/config/config.xml -- --path right/db &> right/log.txt &
    right_pid=$!
    kill -0 $right_pid
    disown $right_pid

    set +m

    while ! left/clickhouse client --query "select 1" ; do kill -0 $left_pid ; echo . ; sleep 1 ; done
    echo left ok

    while ! right/clickhouse client --port 9001 --query "select 1" ; do kill -0 $right_pid ; echo . ; sleep 1 ; done
    echo right ok

    right/clickhouse client --port 9001 --query "create database test" ||:
    right/clickhouse client --port 9001 --query "rename table datasets.hits_v1 to test.hits" ||:
    left/clickhouse client --port 9000 --query "create database test" ||:
    left/clickhouse client --port 9000 --query "rename table datasets.hits_v1 to test.hits" ||:
}
restart

function run_tests
{
    # Just check that the script runs at all
    "$script_dir/perf.py" --help > /dev/null

    # Run the tests
    for test in left/performance/*.xml
    do
        test_name=$(basename $test ".xml")
        "$script_dir/perf.py" "$test" > "$test_name-raw.tsv" 2> "$test_name-err.log" || continue
        right/clickhouse local --file "$test_name-raw.tsv" --structure 'query text, run int, version UInt32, time float' --query "$(cat $script_dir/eqmed.sql)" > "$test_name-report.tsv"
    done
}
run_tests

# Analyze results
result_structure="left float, right float, diff float, rd Array(float), query text"
right/clickhouse local --file '*-report.tsv' -S "$result_structure" --query "select * from table where rd[3] > 0.05 order by rd[3] desc" > flap-prone.tsv
right/clickhouse local --file '*-report.tsv' -S "$result_structure" --query "select * from table where diff > 0.05 and diff > rd[3] order by diff desc" > bad-perf.tsv
grep Exception:[^:] *-err.log > run-errors.log
