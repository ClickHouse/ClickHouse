#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

DATA_BEFORE=`${CLICKHOUSE_CLIENT} --query="SELECT event,value FROM system.events WHERE event IN ('QueryTimeMicroseconds','SelectQueryTimeMicroseconds','InsertQueryTimeMicroseconds') FORMAT CSV"`

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test (k UInt32) ENGINE=MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test (k) SELECT sleep(1)"
${CLICKHOUSE_CLIENT} --query="SELECT sleep(1)" > /dev/null
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test"

DATA_AFTER=`${CLICKHOUSE_CLIENT} --query="SELECT event,value FROM system.events WHERE event IN ('QueryTimeMicroseconds','SelectQueryTimeMicroseconds','InsertQueryTimeMicroseconds') FORMAT CSV"`

declare -A VALUES_BEFORE
VALUES_BEFORE=(["\"QueryTimeMicroseconds\""]="0" ["\"SelectQueryTimeMicroseconds\""]="0" ["\"InsertQueryTimeMicroseconds\""]="0")
declare -A VALUES_AFTER
VALUES_AFTER=(["\"QueryTimeMicroseconds\""]="0" ["\"SelectQueryTimeMicroseconds\""]="0" ["\"InsertQueryTimeMicroseconds\""]="0")

for RES in ${DATA_BEFORE}
do
    IFS=',' read -ra FIELDS <<< ${RES}
    VALUES_BEFORE[${FIELDS[0]}]=${FIELDS[1]}
done

for RES in ${DATA_AFTER}
do
    IFS=',' read -ra FIELDS <<< ${RES}
    VALUES_AFTER[${FIELDS[0]}]=${FIELDS[1]}
done

let QUERY_TIME=${VALUES_AFTER[\"QueryTimeMicroseconds\"]}-${VALUES_BEFORE[\"QueryTimeMicroseconds\"]}
let SELECT_QUERY_TIME=${VALUES_AFTER[\"SelectQueryTimeMicroseconds\"]}-${VALUES_BEFORE[\"SelectQueryTimeMicroseconds\"]}
let INSERT_QUERY_TIME=${VALUES_AFTER[\"InsertQueryTimeMicroseconds\"]}-${VALUES_BEFORE[\"InsertQueryTimeMicroseconds\"]}
if [[ "${QUERY_TIME}" -lt "2000000" ]]; then
    echo "QueryTimeMicroseconds: Fail (${QUERY_TIME})"
else
    echo "QueryTimeMicroseconds: Ok"
fi
if [[ "${SELECT_QUERY_TIME}" -lt "1000000" ]]; then
    echo "SelectQueryTimeMicroseconds: Fail (${SELECT_QUERY_TIME})"
else
    echo "SelectQueryTimeMicroseconds: Ok"
fi
if [[ "${INSERT_QUERY_TIME}" -lt "1000000" ]]; then
    echo "InsertQueryTimeMicroseconds: Fail (${INSERT_QUERY_TIME})"
else
    echo "InsertQueryTimeMicroseconds: Ok"
fi

