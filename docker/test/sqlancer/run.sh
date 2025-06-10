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
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "/workspace/$TEST.out" )  3>&1 1>&2 2>&3 | tee "/workspace/$TEST.err"
    else
        touch "/workspace/$TEST.err" "/workspace/$TEST.out"
        echo "Server is not responding" | tee /workspace/server_crashed.log
    fi
done

ls /workspace
pkill -F /workspace/clickhouse-server.pid || true

for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

/process_sqlancer_result.py || echo -e "failure\tCannot parse results" > /workspace/check_status.tsv
ls /workspace
