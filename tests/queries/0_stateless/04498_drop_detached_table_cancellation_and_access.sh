#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_CANCEL="test_04498_drop_detached_cancel"
TABLE_ACCESS="test_04498_drop_detached_access"
DB_REPLICATED="test_04498_replicated_db"
TABLE_REPLICATED="test_04498_replicated_db_table"
USER_ACCESS="u_04498_drop_detached_${CLICKHOUSE_TEST_UNIQUE_NAME}"
HOLD_QUERY_ID="04498_hold_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DROP_QUERY_ID="04498_drop_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DROP_LOG="${CLICKHOUSE_TEST_UNIQUE_NAME}_04498_drop.log"

function query()
{
    ${CLICKHOUSE_CLIENT} --query "$1"
}

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT infinite_sleep" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id='${DROP_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id='${HOLD_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --multiquery --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS ${TABLE_CANCEL} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --multiquery --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS ${TABLE_ACCESS} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --multiquery --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE IF EXISTS ${DB_REPLICATED}.${TABLE_REPLICATED} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_CANCEL} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_ACCESS} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_REPLICATED} SYNC" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER_ACCESS}" 2>/dev/null ||:
    rm -f "${DROP_LOG}"
}

function wait_for_query()
{
    local query_id="$1"
    for _ in {1..100}
    do
        if [[ "$(query "SELECT count() FROM system.processes WHERE query_id='${query_id}'")" == "1" ]]
        then
            return
        fi
        sleep 0.1
    done

    echo "Query ${query_id} did not appear in system.processes"
    exit 1
}

trap cleanup EXIT
cleanup

query "CREATE TABLE ${TABLE_CANCEL} (number UInt64) ENGINE=MergeTree ORDER BY number"
query "INSERT INTO ${TABLE_CANCEL} VALUES (1)"

query "SYSTEM ENABLE FAILPOINT infinite_sleep"
${CLICKHOUSE_CLIENT} \
    --query_id="${HOLD_QUERY_ID}" \
    --max_execution_time=0 \
    --query "SELECT sleep(0) FROM ${TABLE_CANCEL} FORMAT Null" \
    >/dev/null 2>&1 &
HOLD_PID=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT infinite_sleep PAUSE"

query "DETACH TABLE ${TABLE_CANCEL} PERMANENTLY"

${CLICKHOUSE_CLIENT} \
    --query_id="${DROP_QUERY_ID}" \
    --multiquery \
    --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE ${TABLE_CANCEL} SYNC" \
    >"${DROP_LOG}" 2>&1 &
DROP_PID=$!

wait_for_query "${DROP_QUERY_ID}"

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id='${DROP_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
if wait "${DROP_PID}"
then
    echo "cancelled drop: unexpectedly finished"
    exit 1
fi

query "SYSTEM DISABLE FAILPOINT infinite_sleep"
wait "${HOLD_PID}" 2>/dev/null ||:

query "SELECT count() FROM system.detached_tables WHERE database=currentDatabase() AND table='${TABLE_CANCEL}'"
query "ATTACH TABLE ${TABLE_CANCEL}"
query "SELECT count() FROM ${TABLE_CANCEL}"
query "DROP TABLE ${TABLE_CANCEL} SYNC"

echo "cancelled drop: OK"

query "CREATE TABLE ${TABLE_ACCESS} (number UInt64) ENGINE=MergeTree ORDER BY number"
query "DETACH TABLE ${TABLE_ACCESS} PERMANENTLY"
if [[ "$(query "SELECT count() > 0 FROM system.clusters WHERE cluster='test_shard_localhost'")" == "1" ]]
then
    query "CREATE USER ${USER_ACCESS}"
    query "GRANT CLUSTER ON *.* TO ${USER_ACCESS}"
    query "GRANT DROP TABLE ON ${CLICKHOUSE_DATABASE}.${TABLE_ACCESS} TO ${USER_ACCESS}"

    ${CLICKHOUSE_CLIENT} \
        --user="${USER_ACCESS}" \
        --distributed_ddl_output_mode=none \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE ${TABLE_ACCESS} ON CLUSTER test_shard_localhost SYNC"
else
    ${CLICKHOUSE_CLIENT} \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE ${TABLE_ACCESS} SYNC"
fi

query "SELECT count() FROM system.detached_tables WHERE database=currentDatabase() AND table='${TABLE_ACCESS}'"

echo "on cluster access: OK"

if ${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_REPLICATED} ENGINE=Replicated('/clickhouse/databases/${DB_REPLICATED}', 'shard1', 'replica1')" 2>/dev/null
then
    query "CREATE TABLE ${DB_REPLICATED}.${TABLE_REPLICATED} (number UInt64) ENGINE=MergeTree ORDER BY number"
    query "INSERT INTO ${DB_REPLICATED}.${TABLE_REPLICATED} SELECT number FROM system.numbers LIMIT 6"
    query "DETACH TABLE ${DB_REPLICATED}.${TABLE_REPLICATED} PERMANENTLY"
    ${CLICKHOUSE_CLIENT} \
        --multiquery \
        --query "SET allow_experimental_drop_detached_table=1; DROP DETACHED TABLE ${DB_REPLICATED}.${TABLE_REPLICATED} SYNC"
    query "SELECT count() FROM system.detached_tables WHERE database='${DB_REPLICATED}' AND table='${TABLE_REPLICATED}'"
    query "DROP DATABASE ${DB_REPLICATED} SYNC"
else
    echo "0"
fi

echo "replicated database: OK"
