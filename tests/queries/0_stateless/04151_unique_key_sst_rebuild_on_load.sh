#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
#
# UNIQUE KEY: load-time dense-index lifecycle.
#
# 1. A part that reaches disk without its `unique_key_index.sst` (e.g. a freeze
#    taken before UK shipped, or a sidecar lost on restore) is repaired on load:
#    DETACH/ATTACH re-runs loadDataParts, which rebuilds the SST. The part stays
#    active and its data is fully readable.
# 2. Fail-closed contract: a non-empty UK part whose dense index cannot be
#    rebuilt (missing UK column / unreadable rows / no RocksDB) is detached as
#    broken instead of activated. The rebuild-failure path is covered by the
#    USE_ROCKSDB=0 gtest (writeDenseIndexOnInsert / ensureValidDenseIndex throw)
#    and the CORRUPTED_DATA gtests; reproducing it via stateless filesystem
#    corruption trips the earlier checksum-consistency check first.
#    TODO(unique-key): add a fault-injection stateless variant that loads the
#    columns cleanly but fails the UK rebuild, asserting system.detached_parts.
# 3. A present-but-corrupt SST is NOT trusted on presence. The sidecar carries no
#    checksums.txt entry, so a truncated/corrupt/stale file survives startup and
#    would only fail at probe time. Load-time validation (raw SstFileReader Open +
#    VerifyChecksum + num_entries==rows_count) detects the damage, removes the
#    file, and rebuilds it. Three corruption cases below:
#      a. zero-byte truncation      -> Open corruption; discriminates presence-only.
#      b. partial (half) truncation -> Open corruption (footer at file end lost).
#      c. valid SST with wrong count -> Open + VerifyChecksum PASS; only the
#         num_entries != rows_count check catches it (discriminates that check).
#    A transient (I/O) validation failure is classified separately and must NOT
#    delete/rebuild the file — it raises UNIQUE_KEY_DENSE_INDEX_UNREADABLE so the
#    load fails for retry. Inducing a real transient (FD/OOM) failure in a
#    stateless test is not practical, so that path is covered by code structure +
#    reasoning, not asserted here.
# 4. Readonly startup (`table_readonly = 1`) still validates but cannot
#    remove/rebuild/detach: a corrupt SST fails the ATTACH (fail closed) with
#    the file left untouched; a valid SST loads fine readonly.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_rebuild_load"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_rebuild_load (id UInt64, v String)
    ENGINE = MergeTree
    UNIQUE KEY (id)
    ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
"

echo "INSERT INTO uk_rebuild_load VALUES (10, 'a'), (20, 'b'), (30, 'c')" | ${CLICKHOUSE_CLIENT}

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")

echo "sst_present_before_detach"
[ -f "${DATA_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"

# Detach so the part files are quiescent, drop the SST sidecar, then reattach.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_rebuild_load"
rm -f "${DATA_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE uk_rebuild_load"

# Part survived load and the SST was rebuilt; data is intact.
echo "active_parts_after_attach"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active"

NEW_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
echo "sst_present_after_attach"
[ -f "${NEW_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"

echo "rows_after_attach"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM uk_rebuild_load ORDER BY id"

# --- Corrupt-SST recovery: truncate the (valid, rebuilt) SST to zero bytes to
# simulate a corrupt/truncated sidecar, then reattach. Presence alone must not be
# trusted: load-time validation detects the damage, removes the file, and rebuilds
# it. Discriminator: a zero-byte file would survive the old presence-only fast path
# (present but empty); the fix leaves a present, non-empty, valid SST.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_rebuild_load"
: > "${NEW_PATH}unique_key_index.sst"
echo "sst_nonempty_before_corrupt_attach"
[ -s "${NEW_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"
# The rebuild-from-corrupt path intentionally logs a WARNING ("corrupt/unreadable
# ... removing and rebuilding"); silence server logs for this one ATTACH so the
# harness's stderr check does not flag the expected message.
${CLICKHOUSE_CLIENT} --send_logs_level error --query "ATTACH TABLE uk_rebuild_load"

echo "active_parts_after_corrupt_attach"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active"

FINAL_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
echo "sst_present_after_corrupt_attach"
[ -f "${FINAL_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"
echo "sst_nonempty_after_corrupt_attach"
[ -s "${FINAL_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"

echo "rows_after_corrupt_attach"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM uk_rebuild_load ORDER BY id"

# --- Partial truncation: keep only the first half of the (rebuilt) SST. The
# footer / metaindex live at the file end, so a tail truncation trips a RocksDB
# corruption status at Open — detected and rebuilt. (This is caught before the
# num_entries check; the valid-but-wrong-count case below covers that path.)
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
FULL=$(stat -c%s "${PART_PATH}unique_key_index.sst")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_rebuild_load"
head -c $(( FULL / 2 )) "${PART_PATH}unique_key_index.sst" > "${PART_PATH}unique_key_index.sst.trunc"
mv "${PART_PATH}unique_key_index.sst.trunc" "${PART_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --send_logs_level error --query "ATTACH TABLE uk_rebuild_load"
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
echo "sst_full_size_after_partial_attach"
[ "$(stat -c%s "${PART_PATH}unique_key_index.sst")" -eq "$FULL" ] && echo "yes" || echo "no"
echo "rows_after_partial_attach"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM uk_rebuild_load ORDER BY id"

# --- Valid-but-wrong-count SST: this is what discriminates the num_entries check.
# Build a genuinely valid 1-entry SST from a scratch table and swap it onto the
# 3-row part. It opens and every block checksum verifies, so Open + VerifyChecksum
# both ACCEPT it; only `num_entries (1) != rows_count (3)` flags it as corrupt.
# ATTACH must rebuild it — assert the on-disk SST no longer equals the swapped-in
# 1-entry file. (Without the num_entries check this stays the trusted 1-entry file.)
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_scratch_one"
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_scratch_one (id UInt64, v String)
    ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO uk_scratch_one VALUES (99, 'z')"
ONE_PART=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_scratch_one' AND active")
ONE_SST="${CLICKHOUSE_TMP:-/tmp}/04151_one_entry_${CLICKHOUSE_DATABASE}.sst"
cp "${ONE_PART}unique_key_index.sst" "$ONE_SST"
${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_scratch_one"

PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_rebuild_load"
cp "$ONE_SST" "${PART_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --send_logs_level error --query "ATTACH TABLE uk_rebuild_load"
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_rebuild_load' AND active")
echo "wrongcount_sst_rebuilt"
cmp -s "$ONE_SST" "${PART_PATH}unique_key_index.sst" && echo "no" || echo "yes"
echo "rows_after_wrongcount_attach"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM uk_rebuild_load ORDER BY id"
rm -f "$ONE_SST"

${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_rebuild_load"

# --- Readonly startup: with `table_readonly = 1` the load still VALIDATES the
# SST (read-only I/O) but cannot remove/rebuild/detach (all writes). A corrupt
# SST must fail the ATTACH (fail closed, error names the readonly cause) and the
# file must be left untouched; restoring the valid SST lets the readonly ATTACH
# succeed. Previously the readonly gate skipped validation entirely (fail-open).
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS uk_ro"
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_unique_key = 1;
    CREATE TABLE uk_ro (id UInt64, v String)
    ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO uk_ro VALUES (1, 'x'), (2, 'y')"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE uk_ro MODIFY SETTING table_readonly = 1"
RO_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'uk_ro' AND active")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE uk_ro"
cp "${RO_PATH}unique_key_index.sst" "${RO_PATH}unique_key_index.sst.keep"
: > "${RO_PATH}unique_key_index.sst"
echo "readonly_attach_with_corrupt_sst_fails"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE uk_ro" 2>&1 | grep -q "UNIQUE_KEY_DENSE_INDEX_UNREADABLE" && echo "yes" || echo "no"
echo "corrupt_sst_left_in_place"
[ -f "${RO_PATH}unique_key_index.sst" ] && echo "yes" || echo "no"
mv "${RO_PATH}unique_key_index.sst.keep" "${RO_PATH}unique_key_index.sst"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE uk_ro"
echo "readonly_attach_after_restore"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM uk_ro"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE uk_ro MODIFY SETTING table_readonly = 0"
${CLICKHOUSE_CLIENT} --query "DROP TABLE uk_ro"
