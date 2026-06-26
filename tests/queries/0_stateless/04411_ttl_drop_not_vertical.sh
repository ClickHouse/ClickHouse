#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TABLE=t_ttl_drop_not_vertical

function wait_for_ttl_drop_merge()
{
    for _ in $(seq 1 300); do
        local part_count
        part_count=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count()
            FROM system.parts
            WHERE database = currentDatabase()
                AND table = '${TABLE}'
                AND active")

        if [ "${part_count}" -le "1" ]; then
            break
        fi
        sleep 0.1
    done

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"

    for _ in $(seq 1 60); do
        local merge_count
        merge_count=$(${CLICKHOUSE_CLIENT} -q "
            SELECT count()
            FROM system.part_log
            WHERE database = currentDatabase()
                AND table = '${TABLE}'
                AND event_type = 'MergeParts'
                AND merge_reason = 'TTLDropMerge'")

        if [ "${merge_count}" -gt "0" ]; then
            return
        fi
        sleep 0.1
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
    done
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${TABLE}
    (
        id UInt64,
        d DateTime DEFAULT '2000-01-01 00:00:00',
        c1 UInt64,
        c2 UInt64,
        c3 UInt64,
        c4 UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
    TTL d + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        vertical_merge_optimize_ttl_delete = 1,
        ratio_of_defaults_for_sparse_serialization = 1.0"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP TTL MERGES ${TABLE}"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES ${TABLE}"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${TABLE}
    SELECT number, '2000-01-01 00:00:00', number, number, number, number
    FROM numbers(1000)"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${TABLE}
    SELECT number + 1000, '2000-01-01 00:00:00', number, number, number, number
    FROM numbers(1000)"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES ${TABLE}"
${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES ${TABLE}"

wait_for_ttl_drop_merge

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        merge_algorithm,
        rows
    FROM system.part_log
    WHERE database = currentDatabase()
        AND table = '${TABLE}'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"

TABLE=t_ttl_drop_not_vertical_mixed_ttl

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${TABLE}
    (
        id UInt64,
        d DateTime DEFAULT '2000-01-01 00:00:00',
        c1 UInt64,
        c2 UInt64,
        c3 UInt64,
        c4 UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
    TTL d + INTERVAL 1 DAY, d + INTERVAL 2 DAY RECOMPRESS CODEC(ZSTD)
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        vertical_merge_optimize_ttl_delete = 1,
        ratio_of_defaults_for_sparse_serialization = 1.0"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP TTL MERGES ${TABLE}"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES ${TABLE}"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${TABLE}
    SELECT number, '2000-01-01 00:00:00', number, number, number, number
    FROM numbers(1000)"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${TABLE}
    SELECT number + 1000, '2000-01-01 00:00:00', number, number, number, number
    FROM numbers(1000)"

${CLICKHOUSE_CLIENT} -q "SYSTEM START TTL MERGES ${TABLE}"
${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES ${TABLE}"

wait_for_ttl_drop_merge

${CLICKHOUSE_CLIENT} -q "
    SELECT
        merge_reason,
        merge_algorithm,
        rows
    FROM system.part_log
    WHERE database = currentDatabase()
        AND table = '${TABLE}'
        AND event_type = 'MergeParts'
        AND merge_reason = 'TTLDropMerge'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
