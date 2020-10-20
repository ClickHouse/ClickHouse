#!/bin/bash

set -e -x

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

# install test configs
/usr/share/clickhouse-test/config/install.sh

service clickhouse-server start && sleep 5

if grep -q -- "--use-skip-list" /usr/bin/clickhouse-test; then
    SKIP_LIST_OPT="--use-skip-list"
fi

# We can have several additional options so we path them as array because it's
# more idiologically correct.
read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

clickhouse-test --testname --shard --zookeeper --hung-check --print-time "$SKIP_LIST_OPT" "${ADDITIONAL_OPTIONS[@]}" "$SKIP_TESTS_OPTION" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
