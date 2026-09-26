#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A rolled back part is removable as soon as it appears, so the fixture only survives while the parts
# cleanup does not run: remove_rolled_back_parts_immediately turns that off and the interval settings
# keep the cleanup task away from it.
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple()
    SETTINGS old_parts_lifetime = 10000, max_bytes_to_merge_at_max_space_in_pool = 0,
             remove_rolled_back_parts_immediately = 0,
             merge_tree_clear_old_parts_interval_seconds = 100000,
             cleanup_delay_period = 100000, max_cleanup_delay_period = 100000;
    INSERT INTO TABLE t0 (c0) SELECT 1;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT 2;
    ROLLBACK;

    DETACH TABLE t0 SYNC;
"
${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null -q "ATTACH TABLE t0 AS REPLICATED" 2>&1 |
    grep -c 'transactions were used on this table'
# Nothing was modified: the table still attaches as a MergeTree, and the rolled back row stays
# invisible because its transaction metadata is still on disk.
${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE t0;

    SELECT count() FROM t0;
    SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't0';

    DROP TABLE t0;
"
