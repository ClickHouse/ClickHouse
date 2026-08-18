#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
#
# UNIQUE KEY load-time dense-index lifecycle now that `unique_key_index.sst` is
# recorded in `checksums.txt`:
# 1. Normal round-trip: a valid SST survives DETACH + ATTACH.
# 2. Size-preserving corruption: passes the size check, caught by RocksDB, rebuilt.
#    The table carries a sparsely-serialized all-default UK column, so the rebuild
#    also covers the `ColumnSparse` densify path in `readUniqueKeyColumns`.
# 3. Missing SST: rejected by the size check, part detached as broken.
# 4. Readonly startup: rebuild is impossible, so a corrupt SST fails the ATTACH.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_sst_checksums"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_sst_checksums (a UInt64, id UInt64, v String)
    ENGINE = MergeTree
    UNIQUE KEY (a, id)
    ORDER BY (a, id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, ratio_of_defaults_for_sparse_serialization = 0.9;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO uk_sst_checksums SELECT 0, number, toString(number) FROM numbers(500)"

# Section 1: the SST is written and recorded in checksums.txt (grepped directly
# since CHECK TABLE is rejected for UNIQUE KEY tables). `a` is all-default and
# stored sparsely; `id` keeps the compound key unique. Read the serialization
# kind from system.parts_columns rather than comparing on-disk file sizes, so
# the check is independent of column file naming (e.g. max_file_name_length).
echo "sparse_uk_column_stored"
${CLICKHOUSE_CLIENT} --query "SELECT if(serialization_kind = 'Sparse', 'yes', 'no') FROM system.parts_columns WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active AND column = 'a'"

PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active")

echo "sst_present"
[ -f "${PART_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"

echo "sst_in_checksums"
grep -qa "unique_key_index.sst" "${PART_PATH}checksums.txt" && echo "yes" || echo "no"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_sst_checksums"
${CLICKHOUSE_CLIENT} --send_logs_level error --query "ATTACH TABLE uk_sst_checksums"

echo "active_parts_after_attach"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active"
echo "rows_after_attach"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM uk_sst_checksums"

# Section 2: size-preserving corruption. Overwrite bytes in the middle without
# changing the length, so the size check passes and RocksDB detects the damage.
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active")
FULL=$(stat -c%s "${PART_PATH}unique_key_index.sst")
printf 'XXXXXXXXXXXXXXXX' | dd of="${PART_PATH}unique_key_index.sst" bs=1 seek=$(( FULL / 2 )) conv=notrunc status=none

echo "sst_size_unchanged_after_damage"
[ "$(stat -c%s "${PART_PATH}unique_key_index.sst")" -eq "$FULL" ] && echo "yes" || echo "no"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_sst_checksums"
${CLICKHOUSE_CLIENT} --send_logs_level error --query "ATTACH TABLE uk_sst_checksums"

echo "active_parts_after_corrupt_rebuild"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active"
echo "rows_after_corrupt_rebuild"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM uk_sst_checksums"

# Section 3: missing SST. With a checksum entry this is a plain consistency
# failure, so the part is detached as broken rather than rebuilt.
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_sst_checksums"
rm -f "${PART_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --send_logs_level none --query "ATTACH TABLE uk_sst_checksums" 2>/dev/null

echo "active_parts_after_missing_sst"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active"
echo "detached_parts_after_missing_sst"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.detached_parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_sst_checksums"

# Section 4: readonly startup. Validation still runs (read-only I/O) but rebuild
# is impossible, so a corrupt SST must fail the ATTACH with
# UNIQUE_KEY_DENSE_INDEX_UNREADABLE and leave the file untouched. The corruption
# preserves the size, otherwise the part is rejected before the readonly branch.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_sst_ro"
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_sst_ro (id UInt64, v String)
    ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO uk_sst_ro SELECT number, toString(number) FROM numbers(200)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE uk_sst_ro MODIFY SETTING table_readonly = 1"

RO_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_ro' AND active")
RO_FULL=$(stat -c%s "${RO_PATH}unique_key_index.sst")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_sst_ro"
cp "${RO_PATH}unique_key_index.sst" "${RO_PATH}unique_key_index.sst.keep"
printf 'XXXXXXXXXXXXXXXX' | dd of="${RO_PATH}unique_key_index.sst" bs=1 seek=$(( RO_FULL / 2 )) conv=notrunc status=none

echo "readonly_attach_with_corrupt_sst_fails"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE uk_sst_ro" 2>&1 | grep -q "UNIQUE_KEY_DENSE_INDEX_UNREADABLE" && echo "yes" || echo "no"
echo "corrupt_sst_left_in_place"
[ "$(stat -c%s "${RO_PATH}unique_key_index.sst")" -eq "$RO_FULL" ] && echo "yes" || echo "no"

mv "${RO_PATH}unique_key_index.sst.keep" "${RO_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --send_logs_level error --query "ATTACH TABLE uk_sst_ro"
echo "readonly_attach_after_restore"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM uk_sst_ro"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE uk_sst_ro MODIFY SETTING table_readonly = 0"
${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_sst_ro"
