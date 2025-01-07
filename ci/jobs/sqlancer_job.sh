#!/bin/bash

set -exu

TMP_PATH="./ci/tmp/"
PID_FILE="$TMP_PATH/clickhouse-server.pid"
CLICKHOUSE_BIN="$TMP_PATH/clickhouse"

if [[ -f "$CLICKHOUSE_BIN" ]]; then
    echo "$CLICKHOUSE_BIN exists"
else
    echo "$CLICKHOUSE_BIN does not exists"
    exit 1
fi

chmod +x $CLICKHOUSE_BIN
$CLICKHOUSE_BIN local --version
$CLICKHOUSE_BIN server -P $PID_FILE 1>$TMP_PATH/clickhouse-server.log 2>$TMP_PATH/clickhouse-server.log.err &
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-main

TIMEOUT=300
NUM_QUERIES=1000
NUM_THREADS=10
TESTS=( "TLPGroupBy" "TLPHaving" "TLPWhere" "TLPDistinct" "TLPAggregate" "NoREC" )
echo "${TESTS[@]}"

for TEST in "${TESTS[@]}"; do
    echo "$TEST"
    if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]
    then
        echo "Server is OK"
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "./$TEST.out" )  3>&1 1>&2 2>&3 | tee "./$TEST.err"
    else
        touch "$TMP_PATH/$TEST.err" "$TMP_PATH/$TEST.out"
        echo "Server is not responding" | tee $TMP_PATH/server_crashed.log
    fi
done

ls $TMP_PATH
pkill clickhouse

for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

#$TMP_PATH/process_sqlancer_result.py || echo -e "failure\tCannot parse results" > $TMP_PATH/check_status.tsv
ls $TMP_PATH
