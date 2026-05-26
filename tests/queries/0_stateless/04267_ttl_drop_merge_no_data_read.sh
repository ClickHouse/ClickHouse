#!/usr/bin/env bash
# Tags: no-s3-storage
# Tag no-s3-storage -- merge_tree_clear_old_temporary_directories_interval_seconds
# is not supported for s3 storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that TTLDrop merges do not open source parts or read any data.
# This is important because TTLDrop merges know that all rows in all source
# parts are expired, so the merge should produce an empty part without
# allocating read buffers for source parts.
#
# We avoid OPTIMIZE TABLE FINAL because it always assigns MergeType::Regular,
# which would bypass our TTLDrop short-circuit. Instead we rely on background
# TTL merges (merge_with_ttl_timeout = 0) and wait for them to complete.

# Helper: wait until the background TTL merge completes (at most 1 active part),
# then flush logs and wait for the MergeParts entry to appear.
function wait_for_ttl_merge_and_flush_logs()
{
    local table=$1
    for _ in $(seq 1 300); do
        local part_count
        part_count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active")
        if [ "$part_count" -le "1" ]; then
            break
        fi
        sleep 0.1
    done

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"

    for _ in $(seq 1 60); do
        local count
        count=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'")
        if [ "$count" -gt "0" ]; then
            return
        fi
        sleep 0.1
    done
}

# -------------------------------------------------------------------
# Case 1: Basic TTLDrop — no data should be read
# -------------------------------------------------------------------
echo "-- Case 1: Basic TTLDrop"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_no_read
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_no_read;

    INSERT INTO t_ttl_drop_no_read (id, value) SELECT number, randomString(10000) FROM numbers(1000);
    INSERT INTO t_ttl_drop_no_read (id, value) SELECT number, randomString(10000) FROM numbers(1000);

    SYSTEM START MERGES t_ttl_drop_no_read;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_no_read"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows,
        peak_memory_usage < 50000000
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_no_read'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_no_read;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_no_read;"

# -------------------------------------------------------------------
# Case 2: TTLDrop with projections — projections should be empty
# -------------------------------------------------------------------
echo "-- Case 2: TTLDrop with projections"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_proj
    (
        id UInt64,
        value UInt64,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        PROJECTION proj_sum (SELECT id, sum(value) GROUP BY id)
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_proj;

    INSERT INTO t_ttl_drop_proj (id, value) SELECT number, number FROM numbers(1000);
    INSERT INTO t_ttl_drop_proj (id, value) SELECT number, number FROM numbers(1000);

    SYSTEM START MERGES t_ttl_drop_proj;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_proj"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_proj'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_proj;"

# The resulting part should have no projection data.
${CLICKHOUSE_CLIENT} -q "
    SELECT projections
    FROM system.parts
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_proj'
        AND active;
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_proj;"

# -------------------------------------------------------------------
# Case 3: TTLDrop with skip indexes (set, bloom_filter)
# -------------------------------------------------------------------
echo "-- Case 3: TTLDrop with skip indexes"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_idx
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        INDEX idx_set id TYPE set(100) GRANULARITY 1,
        INDEX idx_bf value TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_idx;

    INSERT INTO t_ttl_drop_idx (id, value) SELECT number, randomString(100) FROM numbers(1000);
    INSERT INTO t_ttl_drop_idx (id, value) SELECT number, randomString(100) FROM numbers(1000);

    SYSTEM START MERGES t_ttl_drop_idx;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_idx"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_drop_idx'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_idx;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_idx;"

# -------------------------------------------------------------------
# Case 4: WHERE-clause TTL does NOT short-circuit
# TTLPartDropMergeSelector assigns TTLDrop based on part_max_ttl even with
# a WHERE clause. Our short-circuit detects the WHERE clause and falls
# through to the normal pipeline, so data IS read.
# -------------------------------------------------------------------
echo "-- Case 4: WHERE-clause TTL is not short-circuited"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_where_no_shortcircuit
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY DELETE WHERE id >= 0
    SETTINGS
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_where_no_shortcircuit;

    INSERT INTO t_ttl_where_no_shortcircuit (id, value) SELECT number, randomString(10) FROM numbers(1000);
    INSERT INTO t_ttl_where_no_shortcircuit (id, value) SELECT number, randomString(10) FROM numbers(1000);

    SYSTEM START MERGES t_ttl_where_no_shortcircuit;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_where_no_shortcircuit"

# Data IS read because the short-circuit detected the WHERE clause and
# fell through to the normal pipeline.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_where_no_shortcircuit'
        AND event_type = 'MergeParts'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_where_no_shortcircuit;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_where_no_shortcircuit;"

# -------------------------------------------------------------------
# Case 5: Table is fully functional after TTLDrop (insert + query + merge)
# -------------------------------------------------------------------
echo "-- Case 5: Table works after TTLDrop"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_drop_then_insert
    (
        id UInt64,
        value String,
        event_time DateTime
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_drop_then_insert;

    -- Insert expired data.
    INSERT INTO t_ttl_drop_then_insert SELECT number, randomString(100), now() - INTERVAL 2 DAY FROM numbers(1000);
    INSERT INTO t_ttl_drop_then_insert SELECT number, randomString(100), now() - INTERVAL 2 DAY FROM numbers(1000);

    SYSTEM START MERGES t_ttl_drop_then_insert;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_drop_then_insert"

# After TTLDrop, insert fresh (non-expired) data.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO t_ttl_drop_then_insert SELECT number, randomString(100), now() FROM numbers(500);
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_then_insert;"

# Merge the fresh data — should work as a normal merge.
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_ttl_drop_then_insert FINAL SETTINGS mutations_sync=1;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_then_insert;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_then_insert;"

# -------------------------------------------------------------------
# Case 6: Rows TTL + column TTL — not short-circuited
# hasOnlyRowsTTL is false when column TTL is present, so data IS read.
# -------------------------------------------------------------------
echo "-- Case 6: Rows TTL + column TTL is not short-circuited"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_col
    (
        id UInt64,
        value String TTL event_time + INTERVAL 1 HOUR,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_col;

    INSERT INTO t_ttl_col (id, value) SELECT number, randomString(10) FROM numbers(1000);
    INSERT INTO t_ttl_col (id, value) SELECT number, randomString(10) FROM numbers(1000);

    SYSTEM START MERGES t_ttl_col;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_col"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_col'
        AND event_type = 'MergeParts'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_col;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_col;"

# -------------------------------------------------------------------
# Case 7: Rows TTL + GROUP BY TTL — not short-circuited
# hasOnlyRowsTTL is false when GROUP BY TTL is present, so data IS read.
# -------------------------------------------------------------------
echo "-- Case 7: Rows TTL + GROUP BY TTL is not short-circuited"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_ttl_groupby
    (
        id UInt64,
        value UInt64,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY GROUP BY id SET value = max(value)
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1;

    SYSTEM STOP MERGES t_ttl_groupby;

    INSERT INTO t_ttl_groupby (id, value) SELECT number, number FROM numbers(1000);
    INSERT INTO t_ttl_groupby (id, value) SELECT number, number FROM numbers(1000);

    SYSTEM START MERGES t_ttl_groupby;
"

wait_for_ttl_merge_and_flush_logs "t_ttl_groupby"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        rows,
        read_rows
    FROM system.part_log
    WHERE
        database = currentDatabase()
        AND table = 't_ttl_groupby'
        AND event_type = 'MergeParts'
    ORDER BY event_time DESC
    LIMIT 1;
"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_groupby;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_groupby;"
