#!/bin/bash

set -ex

install_packages()
{
    dpkg -i package_folder/clickhouse-common-static_*.deb
    dpkg -i package_folder/clickhouse-server_*.deb
    dpkg -i package_folder/clickhouse-client_*.deb
    dpkg -i package_folder/clickhouse-test_*.deb
}

run()
{
    while ! clickhouse-client --port $PORT_PREV --query "SELECT 1"  &>/dev/null; do echo Waiting for port $PORT_PREV ...; sleep 5; done;
    while ! clickhouse-client --port $PORT_CUR --query "SELECT 1" &>/dev/null; do echo Waiting for port $PORT_CUR ...; sleep 5; done;

    clickhouse-performance-test --port $PORT_PREV $PORT_CUR $TESTS_TO_RUN | tee test_output/test_result.json
}

install_packages

run