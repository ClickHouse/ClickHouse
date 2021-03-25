#!/bin/bash

set -x

install_and_run_server() {
    mkdir /unpacked
    tar -xzf /package_folder/shared_build.tgz -C /unpacked --strip 1
    LD_LIBRARY_PATH=/unpacked /unpacked/clickhouse-server --config /unpacked/config/config.xml >/test_output/stderr.log 2>&1 &
}

run_client() {
    for i in {1..100}; do
        sleep 1
        LD_LIBRARY_PATH=/unpacked /unpacked/clickhouse-client --query "select 'OK'" > /test_output/run.log 2> /test_output/clientstderr.log && break
        [[ $i == 100 ]] && echo 'FAIL'
    done
}

install_and_run_server
run_client
mv /var/log/clickhouse-server/clickhouse-server.log /test_output/clickhouse-server.log
/process_split_build_smoke_test_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
