#!/bin/bash

set -e -x

# Choose random timezone for this test run.
TZ="$(grep -v '#' /usr/share/zoneinfo/zone.tab  | awk '{print $3}' | shuf | head -n1)"
echo "Choosen random timezone $TZ"
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

# install test configs
/usr/share/clickhouse-test/config/install.sh

service clickhouse-server start && sleep 5

if cat /usr/bin/clickhouse-test | grep -q -- "--use-skip-list"; then
    SKIP_LIST_OPT="--use-skip-list"
fi

clickhouse-test --testname --shard --zookeeper "$SKIP_LIST_OPT" $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
