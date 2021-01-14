#!/bin/bash

QUERIES

set -e -x

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

service clickhouse-server start && sleep 5

cd /sqlancer/sqlancer-master

export TIMEOUT=60
export NUM_QUERIES=1000

java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere /test_output/TLPWhere.log
java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPGroupBy /test_output/TLPGroupBy.log
java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPHaving /test_output/TLPHaving.log
java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere --oracle TLPGroupBy /test_output/TLPWhereGroupBy.log
java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPDistinct /test_output/TLPDistinct.log
java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPAggregate /test_output/TLPAggregate.log

cp /sqlancer/sqlancer-master/target/surefire-reports/TEST-sqlancer.dbms.TestClickHouse.xml /test_output/result.xml
