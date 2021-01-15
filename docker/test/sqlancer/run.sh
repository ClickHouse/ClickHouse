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

( java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere | tee /test_output/TLPWhere.out )  3>&1 1>&2 2>&3 | tee TLPWhere.err
( java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPGroupBy | tee /test_output/TLPGroupBy.out )  3>&1 1>&2 2>&3 | tee TLPGroupBy.err
( java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPHaving | tee /test_output/TLPHaving.out )  3>&1 1>&2 2>&3 | tee TLPHaving.err
( java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere --oracle TLPGroupBy | tee /test_output/TLPWhereGroupBy.out )  3>&1 1>&2 2>&3 | tee TLPWhereGroupBy.err
( java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPDistinct | tee /test_output/TLPDistinct.out )  3>&1 1>&2 2>&3 | tee TLPDistinct.err
( java -jar target/SQLancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPAggregate | tee /test_output/TLPAggregate.out )  3>&1 1>&2 2>&3 | tee TLPAggregate.err
