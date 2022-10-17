#!/bin/bash
set -exu
trap "exit" INT TERM

/clickhouse server -P /clickhouse-server.pid -L /clickhouse-server.log -E /clickhouse-server.log.err --daemon

for i in `seq 1 30`; do if [[ `wget -q 'localhost:8123' -O-` == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-master

export TIMEOUT=300
export NUM_QUERIES=1000

( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere | tee /test_output/TLPWhere.out )  3>&1 1>&2 2>&3 | tee /test_output/TLPWhere.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPGroupBy | tee /test_output/TLPGroupBy.out )  3>&1 1>&2 2>&3 | tee /test_output/TLPGroupBy.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPHaving | tee /test_output/TLPHaving.out )  3>&1 1>&2 2>&3 | tee /test_output/TLPHaving.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere --oracle TLPGroupBy | tee /test_output/TLPWhereGroupBy.out )  3>&1 1>&2 2>&3 | tee /test_output/TLPWhereGroupBy.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPDistinct | tee /test_output/TLPDistinct.out )  3>&1 1>&2 2>&3 | tee /test_output/TLPDistinct.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPAggregate | tee /test_output/TLPAggregate.out )  3>&1 1>&2 2>&3 | tee /test_output/TLPAggregate.err

pkill -F ./clickhouse-server.pid

for i in `seq 1 30`; do if [[ `wget -q 'localhost:8123' -O-` == 'Ok.' ]]; then sleep 1 ; else break; fi ; done


ls /var/log/clickhouse-server/
tar czf /test_output/logs.tar.gz -C /var/log/clickhouse-server/ .
tail -n 1000 /var/log/clickhouse-server/stderr.log > /test_output/stderr.log
tail -n 1000 /var/log/clickhouse-server/stdout.log > /test_output/stdout.log
tail -n 1000 /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log

/process_sqlancer_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
ls /test_output
