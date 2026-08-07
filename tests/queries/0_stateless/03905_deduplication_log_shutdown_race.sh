#!/usr/bin/env bash
# Tags: race
# Test for a race condition between ALTER TABLE MODIFY SETTING of
# non_replicated_deduplication_window and DROP TABLE / table shutdown.
# The bug: setDeduplicationWindowSize did not check the `stopped` flag,
# so it could create a new current_writer after shutdown() had already
# run, leading to "WriteBuffer is neither finalized nor canceled in
# destructor" assertion.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

NUM_TABLES=5
TIMEOUT=10

for i in $(seq 1 $NUM_TABLES); do
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.dedup_race_${i} (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS non_replicated_deduplication_window = 100
    "
done

function thread_create_drop()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        local i=$((RANDOM % NUM_TABLES + 1))
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dedup_race_${i}" 2>/dev/null
        ${CLICKHOUSE_CLIENT} --query "
            CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.dedup_race_${i} (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS non_replicated_deduplication_window = 100
        " 2>/dev/null
    done
}

function thread_alter_dedup_window()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        local i=$((RANDOM % NUM_TABLES + 1))
        ${CLICKHOUSE_CLIENT} --query "
            ALTER TABLE ${CLICKHOUSE_DATABASE}.dedup_race_${i}
            MODIFY SETTING non_replicated_deduplication_window = $((RANDOM % 200))
        " 2>/dev/null
    done
}

thread_create_drop &
thread_create_drop &
thread_create_drop &
thread_alter_dedup_window &
thread_alter_dedup_window &
thread_alter_dedup_window &
thread_alter_dedup_window &
thread_alter_dedup_window &

wait

for i in $(seq 1 $NUM_TABLES); do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dedup_race_${i}" 2>/dev/null
done

echo "OK"
