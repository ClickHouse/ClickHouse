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


/clickhouse server -P /clickhouse-server.pid -L /clickhouse-server.log -E /clickhouse-server.log.err --daemon

for _ in $(seq 1 30); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-master

export TIMEOUT=300
export NUM_QUERIES=1000

( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere | tee /workspace/TLPWhere.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPWhere.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPGroupBy | tee /workspace/TLPGroupBy.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPGroupBy.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPHaving | tee /workspace/TLPHaving.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPHaving.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPWhere --oracle TLPGroupBy | tee /workspace/TLPWhereGroupBy.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPWhereGroupBy.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPDistinct | tee /workspace/TLPDistinct.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPDistinct.err
( java -jar target/sqlancer-*.jar --num-threads 10 --timeout-seconds $TIMEOUT --num-queries $NUM_QUERIES  --username default --password "" clickhouse --oracle TLPAggregate | tee /workspace/TLPAggregate.out )  3>&1 1>&2 2>&3 | tee /workspace/TLPAggregate.err

pkill -F /clickhouse-server.pid

for i in $(seq 1 30); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

ls /var/log/clickhouse-server/
tar czf /workplace/logs.tar.gz -C /var/log/clickhouse-server/ .
tail -n 1000 /var/log/clickhouse-server/stderr.log > /workplace/stderr.log
tail -n 1000 /var/log/clickhouse-server/stdout.log > /workplace/stdout.log
tail -n 1000 /var/log/clickhouse-server/clickhouse-server.log > /workplace/clickhouse-server.log

/process_sqlancer_result.py || echo -e "failure\tCannot parse results" > /workplace/check_status.tsv
ls /workplace
