#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
#
# UNIQUE KEY: load-time dense-index lifecycle now that `unique_key_index.sst`
# is recorded in `checksums.txt`.
#
# The checksum entry splits the failure modes in two:
#   - A missing or wrongly-sized SST is rejected by `checkConsistencyBase`
#     before the UK validator runs, so the part is detached as broken.
#   - Damage that preserves the file size passes the size check and is caught by
#     `ensureValidDenseIndex`, which removes and rebuilds the index.
#
# 1. Normal round-trip: a valid SST survives DETACH + ATTACH.
# 2. Size-preserving corruption: detected by RocksDB, rebuilt, part activates.
# 3. Missing SST: rejected by the size check, part detached as broken.
# 4. Readonly startup (`table_readonly = 1`): validation still runs (read-only
#    I/O) but a rebuild is impossible, so a corrupt SST fails the ATTACH with
#    UNIQUE_KEY_DENSE_INDEX_UNREADABLE and the file is left untouched.

# `ratio_of_defaults_for_sparse_serialization = 1.0` pins sparse serialization
# off: CI injects a random value for it, and `readUniqueKeyColumns` (the rebuild
# path exercised below) does not handle `ColumnSparse` yet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_sst_checksums"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_sst_checksums (id UInt64, v String)
    ENGINE = MergeTree
    UNIQUE KEY (id)
    ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, ratio_of_defaults_for_sparse_serialization = 1.0;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO uk_sst_checksums SELECT number, toString(number) FROM numbers(500)"

# Section 1: the SST is written and recorded in checksums.txt. `checksums.txt`
# lists every checksummed file, so grepping it proves the sidecar is covered
# (CHECK TABLE is rejected for UNIQUE KEY tables, so we inspect the file
# directly). The size check in `checkConsistencyBase` then relies on this entry -
# exercised by section 3.
echo "sst_present"
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active")
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
# changing the length, so `checkSize` passes and RocksDB's block checksums are
# what detects the damage. The validator then rebuilds the index.
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

# Section 3: missing SST. The checksum entry makes this a plain consistency
# failure - the part is detached as broken instead of being rebuilt.
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_sst_checksums"
rm -f "${PART_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --send_logs_level none --query "ATTACH TABLE uk_sst_checksums" 2>/dev/null

echo "active_parts_after_missing_sst"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums' AND active"
echo "detached_parts_after_missing_sst"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.detached_parts WHERE database = currentDatabase() AND table = 'uk_sst_checksums'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_sst_checksums"

# Section 4: readonly startup. Validation is read-only I/O so it still runs, but
# neither removal nor rebuild is possible: the ATTACH must fail closed with
# UNIQUE_KEY_DENSE_INDEX_UNREADABLE and leave the file untouched. The corruption
# has to preserve the size, otherwise `checkConsistencyBase` rejects the part
# first and the readonly branch is never reached.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_sst_ro"
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_sst_ro (id UInt64, v String)
    ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, ratio_of_defaults_for_sparse_serialization = 1.0;
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
