#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# Reproduces parts left by the mutation bug fixed in #109616: standalone skip-index files
# remain on disk but are absent from checksums.txt. TTL cleanup must still replace the part
# once, remove the orphan files, and record completion instead of selecting it repeatedly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ttl_clear_index_orphan_files SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE ttl_clear_index_orphan_files
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    index_granularity = 100,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0,
    columns_and_secondary_indices_sizes_lazy_calculation = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ttl_clear_index_orphan_files VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2)"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files'")
ACTIVE_PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files' AND active LIMIT 1")
cp "${ACTIVE_PART}skp_idx_idx.idx2" "${DATA_PATH}/saved_idx.idx2"
cp "${ACTIVE_PART}skp_idx_idx.cmrk2" "${DATA_PATH}/saved_idx.cmrk2"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE ttl_clear_index_orphan_files REMOVE TTL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ttl_clear_index_orphan_files DROP INDEX idx SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ttl_clear_index_orphan_files ADD INDEX idx v TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ttl_clear_index_orphan_files MODIFY TTL d + INTERVAL 1 DAY CLEAR INDEX idx SETTINGS materialize_ttl_after_modify = 0"

CORRUPT_PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files' AND active LIMIT 1")
cp "${DATA_PATH}/saved_idx.idx2" "${CORRUPT_PART}skp_idx_idx.idx2"
cp "${DATA_PATH}/saved_idx.cmrk2" "${CORRUPT_PART}skp_idx_idx.cmrk2"

echo "ttl_info_present:"
${CLICKHOUSE_CLIENT} -q "SELECT length(index_clear_ttl_info.max) = 1 FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files' AND active"
echo "orphan_before:"
if compgen -G "${CORRUPT_PART}skp_idx_idx.*" >/dev/null; then echo 1; else echo 0; fi

${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ttl_clear_index_orphan_files FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1, optimize_throw_if_noop = 0"
FIRST_REPLACEMENT=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files' AND active LIMIT 1")
FIRST_REPLACEMENT_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files' AND active LIMIT 1")

echo "orphan_after:"
if compgen -G "${FIRST_REPLACEMENT_PATH}skp_idx_idx.*" >/dev/null; then echo 1; else echo 0; fi

${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ttl_clear_index_orphan_files FINAL SETTINGS enable_ttl_clear_index_merge_type_generation = 1, optimize_skip_merged_partitions = 1, optimize_throw_if_noop = 0"
SECOND_REPLACEMENT=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_orphan_files' AND active LIMIT 1")
echo "no_second_replacement:"
if [[ "$FIRST_REPLACEMENT" == "$SECOND_REPLACEMENT" ]]; then echo 1; else echo 0; fi

echo "check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE ttl_clear_index_orphan_files SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ttl_clear_index_orphan_files"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ttl_clear_index_orphan_files SYNC"
