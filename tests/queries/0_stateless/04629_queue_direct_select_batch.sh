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
