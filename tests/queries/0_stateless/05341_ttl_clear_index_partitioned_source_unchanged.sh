#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-object-storage
# no-replicated-database, no-shared-merge-tree: `OPTIMIZE` without `FINAL` selects TTL merges only on plain `MergeTree`.
# no-object-storage: the test hashes the local files of parts. On object storage those are metadata
# files, and a hardlink rewrites the source's metadata file to count the new reference.

# A dedicated `TTLClearIndex` merge hardlinks the source part's files and writes new copies of the files
# that change. The outdated source parts must keep the same bytes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TABLE=ttl_clear_index_partitioned

# `old_parts_lifetime` keeps the source parts on disk. The size limit stops regular merges, and a
# `TTLClearIndex` merge that hardlinks files ignores it. `merge_with_ttl_timeout` would stop a second
# `TTLClearIndex` merge in the same partition if these merges waited for it.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE}
    (
        d Date,
        k UInt64,
        v UInt64,
        INDEX idx v TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(d)
    ORDER BY k
    TTL d + INTERVAL 1 DAY CLEAR INDEX idx
    SETTINGS
        ttl_clear_index_merges = 0,
        max_number_of_merges_with_ttl_in_pool = 100,
        always_use_copy_instead_of_hardlinks = 0,
        old_parts_lifetime = 3600,
        max_bytes_to_merge_at_min_space_in_pool = 1,
        max_bytes_to_merge_at_max_space_in_pool = 1,
        merge_with_ttl_timeout = 86400,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        min_bytes_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0;
    INSERT INTO ${TABLE} VALUES ('2000-01-05', 1, 1), ('2000-01-06', 2, 2);
    INSERT INTO ${TABLE} VALUES ('2000-01-07', 3, 3);
    INSERT INTO ${TABLE} VALUES ('2000-01-08', 4, 4);
    INSERT INTO ${TABLE} VALUES ('2000-02-05', 5, 5), ('2000-02-06', 6, 6);
    DELETE FROM ${TABLE} WHERE k = 5 SETTINGS lightweight_delete_mode = 'alter_update';"

function hash_part_files()
{
    find "$1" -type f -print0 | sort -z | xargs -0 md5sum
}

declare -A SOURCE_HASHES
while IFS=$'\t' read -r name path; do
    SOURCE_HASHES[${name}]=$(hash_part_files "${path}")
done < <(${CLICKHOUSE_CLIENT} -q "
    SELECT name, path FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE}' AND active FORMAT TSVRaw")

# Each `OPTIMIZE` runs a `TTLClearIndex` merge on one part, unless a background merge is already doing it. `OPTIMIZE ... FINAL`
# waits for those background merges. Then it merges the three cleared parts of 200001 into one part and
# skips the cleared part of 200002.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${TABLE} MODIFY SETTING ttl_clear_index_merges = 1"
for _ in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ${TABLE}"
done
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ${TABLE} FINAL SETTINGS optimize_skip_merged_partitions = 1"

${CLICKHOUSE_CLIENT} -q "
    SELECT partition, count(), sum(secondary_indices_compressed_bytes)
    FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE}' AND active
    GROUP BY partition ORDER BY partition;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = '${TABLE}' AND event_type = 'MergeParts' AND merge_reason = 'TTLClearIndexMerge';"

echo "outdated sources:"
for name in "${!SOURCE_HASHES[@]}"; do
    path=$(${CLICKHOUSE_CLIENT} -q "
        SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE}' AND name = '${name}' AND NOT active")
    if [ -z "${path}" ]; then
        echo "${name} is not outdated"
    elif [ "$(hash_part_files "${path}")" == "${SOURCE_HASHES[${name}]}" ]; then
        echo "unchanged"
    else
        echo "${name} changed"
    fi
done

# Load the cleared parts from disk.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${TABLE}"

# `d` is not in the sorting key, so only the partition min/max index can skip the other partition.
# The deleted row stays hidden only if the cleared part kept `_row_exists`.
${CLICKHOUSE_CLIENT} -q "
    SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${TABLE}';
    SELECT count(), sum(v) FROM ${TABLE} WHERE d < '2000-02-01' SETTINGS force_index_by_date = 1, max_rows_to_read = 4;
    SELECT count(), sum(v) FROM ${TABLE} WHERE d >= '2000-02-01' SETTINGS force_index_by_date = 1, max_rows_to_read = 2;
    CHECK TABLE ${TABLE} SETTINGS check_query_single_value_result = 1;
    DROP TABLE ${TABLE};"
