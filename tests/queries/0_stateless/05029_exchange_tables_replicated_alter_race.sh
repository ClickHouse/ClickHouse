#!/usr/bin/env bash
# Tags: race, zookeeper, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ReplicatedMergeTree variant of 04054_exchange_tables_uuid_race. COMMENT COLUMN takes the local
# fast path in StorageReplicatedMergeTree::alter, while ADD/DROP COLUMN goes through the replication
# log and is applied by executeMetadataAlter in the background, so both the foreground and the
# background metadata writes race with EXCHANGE TABLES here. None of them should throw.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t1_05029 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t1_05029', '1') ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t2_05029 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t2_05029', '1') ORDER BY x"

TIMEOUT=30

function thread_exchange()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CLIENT} -q "EXCHANGE TABLES t1_05029 AND t2_05029" 2>/dev/null ||:
    done
}

# SharedMergeTree serializes metadata alters: a racing alter can be rejected with a retryable
# CANNOT_ASSIGN_ALTER whose message says to retry, which is expected and filtered out below.
function thread_comment()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    local i=0
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        i=$((i + 1))
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t1_05029 COMMENT COLUMN x 'c${i}'" 2>&1 | grep -Fa "Exception: " | grep -Fav "retry this" ||:
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t2_05029 COMMENT COLUMN x 'c${i}'" 2>&1 | grep -Fa "Exception: " | grep -Fav "retry this" ||:
    done
}

function thread_columns()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t1_05029 ADD COLUMN IF NOT EXISTS y UInt8" 2>&1 | grep -Fa "Exception: " | grep -Fav "retry this" ||:
        ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t1_05029 DROP COLUMN IF EXISTS y" 2>&1 | grep -Fa "Exception: " | grep -Fav "retry this" ||:
    done
}

thread_exchange &
thread_exchange &
thread_comment &
thread_columns &

wait

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t1_05029"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t2_05029"

echo "OK"
