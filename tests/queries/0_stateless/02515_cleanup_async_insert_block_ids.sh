#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, long, no-fasttest, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Check that if the background cleanup thread works correctly.
CLICKHOUSE_TEST_ZOOKEEPER_PREFIX="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_async_insert_cleanup SYNC;
    CREATE TABLE t_async_insert_cleanup (
        KeyID UInt32
    ) Engine = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_async_insert_cleanup', '{replica}')
    ORDER BY (KeyID) SETTINGS cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration=0, replicated_deduplication_window_for_async_inserts=10
"

for i in {1..100}; do
    $CLICKHOUSE_CLIENT --async_insert 1 --async_insert_deduplicate 1 --wait_for_async_insert 0 --query "insert into t_async_insert_cleanup values ($i), ($((i + 1))), ($((i + 2)))"
done

sleep 1

old_answer=$($CLICKHOUSE_CLIENT --query "SELECT count(*) FROM system.zookeeper WHERE path like '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_async_insert_cleanup/async_blocks%' settings allow_unrestricted_reads_from_keeper = 'true'")

for i in {1..300}; do
    answer=$($CLICKHOUSE_CLIENT --query "SELECT count(*) FROM system.zookeeper WHERE path like '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_async_insert_cleanup/async_blocks%' settings allow_unrestricted_reads_from_keeper = 'true'")
    if [ $answer == '10' ]; then
        $CLICKHOUSE_CLIENT --query "DROP TABLE t_async_insert_cleanup SYNC;"
        exit 0
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT --query "SELECT count(*) FROM t_async_insert_cleanup"
echo $old_answer
$CLICKHOUSE_CLIENT --query "SELECT count(*) FROM system.zookeeper WHERE path like '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_async_insert_cleanup/async_blocks%' settings allow_unrestricted_reads_from_keeper = 'true'"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_async_insert_cleanup SYNC;"
