#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_cleanup SYNC;
    SET enable_lightweight_update = 1;

    CREATE TABLE t_lwu_cleanup (id UInt64, c1 UInt64, c2 Int16)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_cleanup/', '1')
    ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        cleanup_delay_period = 1,
        max_cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 1,
        shared_merge_tree_disable_merges_and_mutations_assignment = 1;

    INSERT INTO t_lwu_cleanup SELECT number, number, number FROM numbers(20);
    INSERT INTO t_lwu_cleanup SELECT number, number, number FROM numbers(100, 10);

    UPDATE t_lwu_cleanup SET c2 = c1 * c1 * 10 WHERE id < 10;

    SELECT sum(c2) FROM t_lwu_cleanup SETTINGS apply_patch_parts = 0;
    SELECT sum(c2) FROM t_lwu_cleanup SETTINGS apply_patch_parts = 1;

    SYSTEM SYNC REPLICA t_lwu_cleanup;
    OPTIMIZE TABLE t_lwu_cleanup PARTITION ID 'all' FINAL;

    SELECT sum(c2) FROM t_lwu_cleanup SETTINGS apply_patch_parts = 0;
    SELECT sum(c2) FROM t_lwu_cleanup SETTINGS apply_patch_parts = 1;
"

for _ in {0..50}; do
    res=`$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_cleanup' AND active AND startsWith(name, 'patch')"`
    if [[ $res == "0" ]]; then
        break
    fi
    sleep 1.0
done

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_cleanup' AND active AND startsWith(name, 'patch');
    DROP TABLE t_lwu_cleanup SYNC;
"
