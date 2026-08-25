#!/usr/bin/env bash
# Tags: race, no-parallel
# Test for a race condition between reading system.parts and removing parts.
# The race was in DataPartStorageOnDiskBase::remove() modifying part_dir
# while getFullPath() was reading it concurrently.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "DROP TABLE IF EXISTS part_race"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "CREATE TABLE part_race (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x % 10
    SETTINGS old_parts_lifetime = 0, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration = 0, max_cleanup_delay_period = 0"

TIMEOUT=30

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    local i=0
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO part_race SELECT $i" &>/dev/null
        ((i++)) || true
    done
}

function thread_drop_partition()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "ALTER TABLE part_race DROP PARTITION ID '$((RANDOM % 10))'" &>/dev/null
        sleep 0.0$RANDOM
    done
}

function thread_select_parts()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT name, path FROM system.parts WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 'part_race' FORMAT Null" &>/dev/null
    done
}

# Start multiple instances of each thread
thread_insert &
thread_insert &

thread_drop_partition &
thread_drop_partition &

thread_select_parts &
thread_select_parts &
thread_select_parts &
thread_select_parts &

wait

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "DROP TABLE part_race"

echo "OK"
