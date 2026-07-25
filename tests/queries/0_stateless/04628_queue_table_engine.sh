#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

queue_settings="--allow_experimental_queue_table_engine=1"

cleanup()
{
    $CLICKHOUSE_CLIENT $queue_settings -q "
        DROP TABLE IF EXISTS queue_mv;
        DROP TABLE IF EXISTS queue_failed_mv;
        DROP TABLE IF EXISTS queue_target;
        DROP TABLE IF EXISTS queue_failed_target;
        DROP TABLE IF EXISTS queue_source;
        DROP TABLE IF EXISTS queue_retry_source;
        DROP TABLE IF EXISTS queue_disabled;" >/dev/null 2>&1
}

wait_for_count()
{
    local table=$1
    local expected=$2
    for _ in $(seq 1 100)
    do
        if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${table}")" = "$expected" ]
        then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

trap cleanup EXIT
cleanup

if $CLICKHOUSE_CLIENT -q "
    CREATE TABLE queue_disabled (id UInt64)
    ENGINE = Queue()" >/dev/null 2>&1
then
    echo "experimental gate: failed"
else
    echo "experimental gate: passed"
fi

if $CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_disabled (id UInt64)
    ENGINE = Queue(0)" >/dev/null 2>&1
then
    echo "argument validation: failed"
else
    echo "argument validation: passed"
fi

$CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_source
    (
        id UInt64,
        value String
    )
    ENGINE = Queue(3600, 2, 60000);

    SYSTEM STOP queue_source;

    CREATE TABLE queue_target
    (
        id UInt64,
        value String
    )
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_mv TO queue_target
    AS SELECT id, value FROM queue_source;

    INSERT INTO queue_source VALUES
        (3, 'three'),
        (1, 'one'),
        (5, 'five'),
        (2, 'two'),
        (4, 'four');"

echo "rows before refresh: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_target")"

$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH queue_source"
wait_for_count queue_target 2
echo "rows after first batch: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_target")"

$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH queue_source"
wait_for_count queue_target 4
echo "rows after second batch: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_target")"

$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH queue_source"
wait_for_count queue_target 5

$CLICKHOUSE_CLIENT -q "
    SELECT groupArray((id, value))
    FROM (SELECT * FROM queue_target ORDER BY id)"

$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH queue_source"
sleep 0.2
echo "rows after second refresh: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_target")"

$CLICKHOUSE_CLIENT -q "
    SELECT count() = 1
    FROM system.tables
    WHERE database = currentDatabase()
      AND name LIKE '.inner%queue%'
      AND create_table_query LIKE '%ReplacingMergeTree(_queue_version, _queue_is_deleted)%'
      AND create_table_query LIKE '%TTL _queue_created_at + toIntervalSecond(3600)%'"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE queue_mv;
    DROP TABLE queue_target;
    DROP TABLE queue_source;"

$CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_retry_source
    (
        id UInt64,
        value UInt64
    )
    ENGINE = Queue(3600, 100, 60000);

    SYSTEM STOP queue_retry_source;

    CREATE TABLE queue_failed_target
    (
        id UInt64,
        value UInt64,
        CONSTRAINT reject_two CHECK value != 2
    )
    ENGINE = MergeTree
    ORDER BY id;

    CREATE MATERIALIZED VIEW queue_failed_mv TO queue_failed_target
    AS SELECT id, value FROM queue_retry_source;

    INSERT INTO queue_retry_source VALUES (1, 1), (2, 2), (3, 3);
    SYSTEM REFRESH queue_retry_source;"

sleep 0.2
echo "rows after failed delivery: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_failed_target")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE queue_failed_mv;
    DROP TABLE queue_failed_target;

    CREATE TABLE queue_target
    (
        id UInt64,
        value UInt64
    )
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_mv TO queue_target
    AS SELECT id, value FROM queue_retry_source;

    SYSTEM REFRESH queue_retry_source;"

wait_for_count queue_target 3
echo "rows after retry: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_target")"
