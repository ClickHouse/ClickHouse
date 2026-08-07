#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

queue_settings="--allow_experimental_queue_table_engine=1"

cleanup()
{
    $CLICKHOUSE_CLIENT $queue_settings -q "
        DROP TABLE IF EXISTS queue_mv;
        DROP TABLE IF EXISTS queue_mv_second;
        DROP TABLE IF EXISTS queue_failed_mv;
        DROP TABLE IF EXISTS queue_healthy_mv;
        DROP TABLE IF EXISTS queue_retry_mv;
        DROP TABLE IF EXISTS queue_target;
        DROP TABLE IF EXISTS queue_target_second;
        DROP TABLE IF EXISTS queue_failed_target;
        DROP TABLE IF EXISTS queue_healthy_target;
        DROP TABLE IF EXISTS queue_retry_target;
        DROP TABLE IF EXISTS queue_rmv;
        DROP TABLE IF EXISTS queue_rmv_target;
        DROP TABLE IF EXISTS queue_source;
        DROP TABLE IF EXISTS queue_retry_source;
        DROP TABLE IF EXISTS queue_rmv_source;
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
    AS SELECT id, value FROM queue_source
    SETTINGS queue_consumer_group = 'first';

    CREATE TABLE queue_target_second
    (
        id UInt64,
        value String
    )
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_mv_second TO queue_target_second
    AS SELECT id, value FROM queue_source
    SETTINGS queue_consumer_group = 'second';

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
echo "rows in second group: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_target_second")"

$CLICKHOUSE_CLIENT -q "
    SELECT count() = 2
    FROM system.tables
    WHERE database = currentDatabase()
      AND name LIKE '.inner%queue%'
      AND create_table_query LIKE '%ReplacingMergeTree(_queue_version, _queue_is_deleted)%'
      AND create_table_query LIKE '%TTL _queue_created_at + toIntervalSecond(3600)%'"

echo "direct main rows: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_source")"
echo "direct main rows again: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_source")"

$CLICKHOUSE_CLIENT -q "
    SELECT id
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'direct',
        queue_commit_on_select = 1,
        queue_max_batch_size = 1
    FORMAT Null"
echo "direct group after one: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS queue_consumer_group = 'direct'")"

$CLICKHOUSE_CLIENT -q "
    SELECT id
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'direct',
        queue_commit_on_select = 1,
        queue_max_batch_size = 1000
    FORMAT Null"
echo "direct group after all: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS queue_consumer_group = 'direct'")"
echo "main rows after direct commit: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_source")"

if $CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'direct',
        queue_commit_on_select = 1" >/dev/null 2>&1
then
    echo "committing aggregation: failed"
else
    echo "committing aggregation: rejected"
fi

if $CLICKHOUSE_CLIENT -q "
    SELECT throwIf(id > 0)
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'failed-select',
        queue_commit_on_select = 1,
        queue_max_batch_size = 1000
    FORMAT Null" >/dev/null 2>&1
then
    echo "failed select: unexpectedly succeeded"
else
    echo "failed select pending rows: $($CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM queue_source
        SETTINGS queue_consumer_group = 'failed-select'")"
fi

echo "reset earliest rows: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'direct',
        queue_consumer_offset = 'earliest',
        queue_reset_consumer_offset = 1")"
echo "reset latest rows: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'direct',
        queue_consumer_offset = 'latest',
        queue_reset_consumer_offset = 1")"

$CLICKHOUSE_CLIENT -q "INSERT INTO queue_source VALUES (6, 'six')"
echo "latest group after insert: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS queue_consumer_group = 'direct'")"
echo "reset earliest after insert: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_source
    SETTINGS
        queue_consumer_group = 'direct',
        queue_consumer_offset = 'earliest',
        queue_reset_consumer_offset = 1")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE queue_mv;
    DROP TABLE queue_mv_second;
    DROP TABLE queue_target;
    DROP TABLE queue_target_second;
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
    AS SELECT id, value FROM queue_retry_source
    SETTINGS queue_consumer_group = 'blocked';

    CREATE TABLE queue_healthy_target
    (
        id UInt64,
        value UInt64
    )
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_healthy_mv TO queue_healthy_target
    AS SELECT id, value FROM queue_retry_source
    SETTINGS queue_consumer_group = 'healthy';

    INSERT INTO queue_retry_source VALUES (1, 1), (2, 2), (3, 3);
    SYSTEM REFRESH queue_retry_source;"

sleep 0.2
echo "rows after failed delivery: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_failed_target")"
wait_for_count queue_healthy_target 3
echo "rows in healthy group: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_healthy_target")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE queue_failed_mv;
    DROP TABLE queue_failed_target;

    CREATE TABLE queue_retry_target
    (
        id UInt64,
        value UInt64
    )
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_retry_mv TO queue_retry_target
    AS SELECT id, value FROM queue_retry_source
    SETTINGS queue_consumer_group = 'blocked';

    SYSTEM REFRESH queue_retry_source;"

wait_for_count queue_retry_target 3
echo "rows after retry: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_retry_target")"

$CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_rmv_source
    (
        id UInt64,
        value String
    )
    ENGINE = Queue(3600, 1000, 60000);

    SYSTEM STOP queue_rmv_source;

    CREATE TABLE queue_rmv_target
    (
        id UInt64,
        value String
    )
    ENGINE = MergeTree
    ORDER BY id;

    CREATE MATERIALIZED VIEW queue_rmv
    REFRESH EVERY 1 YEAR APPEND TO queue_rmv_target
    AS SELECT id, value
    FROM queue_rmv_source
    SETTINGS
        queue_consumer_group = 'rmv',
        queue_commit_on_select = 1,
        queue_max_batch_size = 1000;

    INSERT INTO queue_rmv_source VALUES (1, 'one'), (2, 'two'), (3, 'three');

    SYSTEM REFRESH VIEW queue_rmv;
    SYSTEM WAIT VIEW queue_rmv;"

echo "rmv rows after refresh: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_rmv_target")"

$CLICKHOUSE_CLIENT -q "
    SYSTEM REFRESH VIEW queue_rmv;
    SYSTEM WAIT VIEW queue_rmv;"
echo "rmv rows after second refresh: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_rmv_target")"
