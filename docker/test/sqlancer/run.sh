#!/bin/bash

set -e -x

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

service clickhouse-server start && sleep 5

cd /sqlancer/sqlancer-master
CLICKHOUSE_AVAILABLE=true mvn -Dtest=TestClickHouse test

cp /sqlancer/sqlancer-master/target/surefire-reports/TEST-sqlancer.dbms.TestClickHouse.xml /test_output/result.xml