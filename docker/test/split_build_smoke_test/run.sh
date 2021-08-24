#!/bin/bash

set -x

install_and_run_server() {
    mkdir /unpacked
    tar -xzf /package_folder/shared_build.tgz -C /unpacked --strip 1
    LD_LIBRARY_PATH=/unpacked /unpacked/clickhouse-server --config /unpacked/config/config.xml >/var/log/clickhouse-server/stderr.log 2>&1 &
}

run_client() {
    for i in {1..100}; do
        sleep 1
        LD_LIBRARY_PATH=/unpacked /unpacked/clickhouse-client --query "select 'OK'" 2>/var/log/clickhouse-server/clientstderr.log && break
        [[ $i == 100 ]] && echo 'FAIL'
    done
}

install_and_run_server
run_client
