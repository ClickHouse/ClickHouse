#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

queue_settings="--allow_experimental_queue_table_engine=1"

cleanup()
{
    $CLICKHOUSE_CLIENT $queue_settings -q "
        DROP TABLE IF EXISTS queue_filtered_mv;
        DROP TABLE IF EXISTS queue_filtered_target;
        DROP TABLE IF EXISTS queue_starvation_mv;
        DROP TABLE IF EXISTS queue_starvation_target;
        DROP TABLE IF EXISTS queue_starvation_source;
        DROP TABLE IF EXISTS queue_starvation_direct;
        DROP TABLE IF EXISTS queue_direct_batch;" >/dev/null 2>&1
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_direct_batch (id UInt64)
    ENGINE = Queue(3600, 1000, 60000);

    SYSTEM STOP queue_direct_batch;

    INSERT INTO queue_direct_batch VALUES (1), (2), (3);"

$CLICKHOUSE_CLIENT -q "
    SELECT id
    FROM queue_direct_batch
    WHERE id = 1
    SETTINGS
        queue_consumer_group = 'filtered',
        queue_commit_on_select = 1,
        queue_max_batch_size = 3
    FORMAT Null"

echo "pending after filtered direct commit: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_direct_batch
    SETTINGS queue_consumer_group = 'filtered'")"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE queue_filtered_target (id UInt64)
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_filtered_mv TO queue_filtered_target
    AS SELECT id
    FROM queue_direct_batch
    WHERE id = 1
    SETTINGS queue_consumer_group = 'filtered-mv';

    SYSTEM REFRESH queue_direct_batch;"

sleep 0.2
echo "filtered mv target rows: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_filtered_target")"
echo "pending after filtered mv: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_direct_batch
    SETTINGS queue_consumer_group = 'filtered-mv'")"


$CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_starvation_direct (id UInt64)
    ENGINE = Queue(3600, 2, 60000);

    SYSTEM STOP queue_starvation_direct;

    INSERT INTO queue_starvation_direct
    SELECT number
    FROM numbers(1, 6);"

$CLICKHOUSE_CLIENT -q "
    SELECT id
    FROM queue_starvation_direct
    WHERE id >= 3
    SETTINGS
        queue_consumer_group = 'starvation-direct',
        queue_commit_on_select = 1,
        queue_max_batch_size = 2
    FORMAT Null"

echo "direct pending after first result batch: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_starvation_direct
    SETTINGS queue_consumer_group = 'starvation-direct'")"

$CLICKHOUSE_CLIENT -q "
    SELECT id
    FROM queue_starvation_direct
    WHERE id >= 3
    SETTINGS
        queue_consumer_group = 'starvation-direct',
        queue_commit_on_select = 1,
        queue_max_batch_size = 2
    FORMAT Null"

echo "direct pending after second result batch: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_starvation_direct
    SETTINGS queue_consumer_group = 'starvation-direct'")"

$CLICKHOUSE_CLIENT $queue_settings -q "
    CREATE TABLE queue_starvation_source (id UInt64)
    ENGINE = Queue(3600, 2, 60000);

    SYSTEM STOP queue_starvation_source;

    CREATE TABLE queue_starvation_target (id UInt64)
    ENGINE = Memory;

    CREATE MATERIALIZED VIEW queue_starvation_mv TO queue_starvation_target
    AS SELECT id
    FROM queue_starvation_source
    WHERE id >= 3
    SETTINGS queue_consumer_group = 'starvation-mv';

    INSERT INTO queue_starvation_source
    SELECT number
    FROM numbers(1, 6);

    SYSTEM REFRESH queue_starvation_source;"

for _ in $(seq 1 100)
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_starvation_target")" = "2" ] && break
    sleep 0.05
done

echo "mv rows after first result batch: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_starvation_target")"
echo "mv pending after first result batch: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_starvation_source
    SETTINGS queue_consumer_group = 'starvation-mv'")"

$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH queue_starvation_source"

for _ in $(seq 1 100)
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_starvation_target")" = "4" ] && break
    sleep 0.05
done

echo "mv rows after second result batch: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM queue_starvation_target")"
echo "mv pending after second result batch: $($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM queue_starvation_source
    SETTINGS queue_consumer_group = 'starvation-mv'")"

if $CLICKHOUSE_CLIENT -q "
    SELECT id
    FROM queue_direct_batch
    SETTINGS
        queue_consumer_group = 'invalid-reset',
        queue_commit_on_select = 1,
        queue_reset_consumer_offset = 1
    FORMAT Null" >/dev/null 2>&1
then
    echo "reset and commit: failed"
else
    echo "reset and commit: rejected"
fi
