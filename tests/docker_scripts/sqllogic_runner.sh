#!/bin/bash

set -exu
trap "exit" INT TERM

echo "ENV"
env

# fail on errors, verbose and export all env variables
set -e -x -a

echo "Current directory"
pwd
echo "Files in current directory"
ls -la ./
echo "Files in root directory"
ls -la /
echo "Files in /repo/tests directory"
ls -la /repo/tests
echo "Files in /repo/tests/sqllogic directory"
ls -la /repo/tests/sqllogic
echo "Files in /package_folder directory"
ls -la /package_folder
echo "Files in /test_output"
ls -la /test_output
echo "File in /sqllogictest"
ls -la /sqllogictest

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

# install test configs
# /clickhouse-tests/config/install.sh

sudo clickhouse start

sleep 5
for _ in $(seq 1 60); do if [[ $(wget --timeout=1 -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

function run_tests()
{
    set -x

    cd /test_output

    /repo/tests/sqllogic/runner.py --help 2>&1 \
        | ts '%Y-%m-%d %H:%M:%S'

    mkdir -p /test_output/self-test
    /repo/tests/sqllogic/runner.py --log-file /test_output/runner-self-test.log \
        self-test \
        --self-test-dir /repo/tests/sqllogic/self-test \
        --out-dir /test_output/self-test \
        2>&1 \
        | ts '%Y-%m-%d %H:%M:%S'

    cat /test_output/self-test/check_status.tsv >> /test_output/check_status.tsv
    cat /test_output/self-test/test_results.tsv >> /test_output/test_results.tsv ||:
    tar -zcvf self-test.tar.gz self-test 1>/dev/null

    if [ -d /sqllogictest ]
    then
        mkdir -p /test_output/statements-test
        /repo/tests/sqllogic/runner.py \
        --log-file /test_output/runner-statements-test.log \
        --log-level info \
            statements-test \
            --input-dir /sqllogictest \
            --out-dir /test_output/statements-test \
            2>&1 \
            | ts '%Y-%m-%d %H:%M:%S'

        cat /test_output/statements-test/check_status.tsv >> /test_output/check_status.tsv
        cat /test_output/statements-test/test_results.tsv >> /test_output/test_results.tsv
        tar -zcvf statements-check.tar.gz statements-test 1>/dev/null

        mkdir -p /test_output/complete-test
        /repo/tests/sqllogic/runner.py \
        --log-file /test_output/runner-complete-test.log \
        --log-level info \
            complete-test \
            --input-dir /sqllogictest \
            --out-dir /test_output/complete-test \
            2>&1 \
            | ts '%Y-%m-%d %H:%M:%S'

        cat /test_output/complete-test/check_status.tsv >> /test_output/check_status.tsv
        cat /test_output/complete-test/test_results.tsv >> /test_output/test_results.tsv
        tar -zcvf complete-check.tar.gz complete-test 1>/dev/null
    fi
}

export -f run_tests

run_tests

#/process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

clickhouse-client -q "system flush logs" ||:

# Stop server so we can safely read data with clickhouse-local.
# Why do we read data with clickhouse-local?
# Because it's the simplest way to read it when server has crashed.
sudo clickhouse stop ||:

for _ in $(seq 1 60); do if [[ $(wget --timeout=1 -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

rg -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server.log ||:
zstd < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.zst &

# Compressed (FIXME: remove once only github actions will be left)
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:
