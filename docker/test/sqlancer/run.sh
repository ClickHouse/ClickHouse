#!/bin/bash
set -exu
trap "exit" INT TERM

function wget_with_retry
{
    for _ in 1 2 3 4; do
        if wget -nv -nd -c "$1";then
            return 0
        else
            sleep 0.5
        fi
    done
    return 1
}

if [ -z ${BINARY_URL_TO_DOWNLOAD+x} ]
then
    echo "No BINARY_URL_TO_DOWNLOAD provided."
else
    wget_with_retry "$BINARY_URL_TO_DOWNLOAD"
    chmod +x /clickhouse
fi

if [[ -f "/clickhouse" ]]; then
    echo "/clickhouse exists"
else
    exit 1
fi

cd /workspace
/clickhouse server -P /workspace/clickhouse-server.pid -L /workspace/clickhouse-server.log -E /workspace/clickhouse-server.log.err --daemon

for _ in $(seq 1 30); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-master

export TIMEOUT=300
export NUM_QUERIES=1000
export NUM_THREADS=30

( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere | tee /workspace/TLPWhere.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPWhere.err
( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPGroupBy | tee /workspace/TLPGroupBy.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPGroupBy.err
( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPHaving | tee /workspace/TLPHaving.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPHaving.err
( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere --oracle TLPGroupBy | tee /workspace/TLPWhereGroupBy.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPWhereGroupBy.err
( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPDistinct | tee /workspace/TLPDistinct.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPDistinct.err
( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPAggregate | tee /workspace/TLPAggregate.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPAggregate.err
( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads $NUM_THREADS --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPAggregate | tee /workspace/NoREC.out )  3>&1 1>&2 2>&3 | tee /workspace/NoREC.err

ls /workspace
pkill -F /workspace/clickhouse-server.pid

for _ in $(seq 1 30); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

/process_sqlancer_result.py || echo -e "failure\tCannot parse results" > /workspace/check_status.tsv
ls /workspace
