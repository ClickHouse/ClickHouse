#!/bin/bash

set -x

install_and_run_server() {
    tar -xzf package_folder/shared_build.tgz -C package_folder --strip 1
    LD_LIBRARY_PATH=/package_folder /package_folder/clickhouse-server --config /package_folder/config/config.xml >/var/log/clickhouse-server/stderr.log 2>&1 &
    sleep 5
}

run_client() {
    LD_LIBRARY_PATH=/package_folder /package_folder/clickhouse-client --query \"select 'OK'\" 2>/var/log/clickhouse-server/clientstderr.log || echo 'FAIL'
}

install_and_run_server
run_client
