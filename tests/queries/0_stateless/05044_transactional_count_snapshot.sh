#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-async-insert
# no-ordinary-database: the test uses transactions
# no-replicated-database: MergeTree becomes ReplicatedMergeTree there, and a transaction that writes is rejected for it
# no-async-insert: every INSERT has to produce its own part

# `count()` served from part metadata must respect the transaction snapshot rather than the set of
# currently active parts. Three concurrent transactions are used:
#   tx1 drops the first part, tx2 adds two parts, and neither commits;
#   tx3 then counts, and must still see the part tx1 is dropping while not seeing the parts tx2 is adding.
# A non-transactional read has no snapshot, so it observes the uncommitted work of both.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

set -eu

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS txn_count_snapshot"

# `s` is left at its default in every row, so it is sparse-encoded with an exact `num_defaults`. That makes
# the table usable both by the plain trivial count, which is served from `totalRows()`, and by the
# sparsity-filter variant, which is served from the per-column `num_defaults`.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE txn_count_snapshot (n Int64, s UInt32)
        ENGINE = MergeTree ORDER BY n
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1,
                 ratio_of_defaults_for_sparse_serialization = 0.9,
                 compute_exact_num_defaults_for_sparse_columns = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO txn_count_snapshot VALUES (1, 0)"
$CLICKHOUSE_CLIENT -q "INSERT INTO txn_count_snapshot VALUES (2, 0)"
$CLICKHOUSE_CLIENT -q "INSERT INTO txn_count_snapshot VALUES (3, 0)"

FIRST_PART=$($CLICKHOUSE_CLIENT -q "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'txn_count_snapshot' AND active
    ORDER BY min_block_number LIMIT 1")

echo 'parts and sparse encoding of s'
$CLICKHOUSE_CLIENT -q "
    SELECT count(), countIf(serialization_kind = 'Sparse') FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'txn_count_snapshot' AND active AND column = 's'"

# The payload of tx1 and tx2 runs now, but neither is committed until the counts below have been taken.
tx 1 "BEGIN TRANSACTION"
tx 1 "ALTER TABLE txn_count_snapshot DROP PART '$FIRST_PART'"

tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO txn_count_snapshot VALUES (4, 0)"
tx 2 "INSERT INTO txn_count_snapshot VALUES (5, 0)"

tx 3 "BEGIN TRANSACTION"

# Without these the counts below would be correct for the wrong reason: an optimization that declined and
# fell back to a plain read cannot get the snapshot wrong in the first place.
# The plain variant is implemented on both planning paths, so it must always be applied. The sparsity variant is
# implemented only with the analyzer enabled, so rather than skipping the whole test with the old analyzer,
# assert that it is applied exactly when `enable_analyzer` is on. That keeps the check meaningful in both
# configurations and the expected output identical. If the rewrite is ever implemented for the old analyzer
# this assertion starts failing, which is the right signal rather than a false alarm.
echo 'the plain optimization must be applied inside the transaction, the sparsity one when it is available'
tx 3 "SELECT countIf(explain LIKE '%Optimized trivial count%') > 0 FROM (EXPLAIN SELECT count() FROM txn_count_snapshot SETTINGS optimize_trivial_count_query = 1)"
tx 3 "SELECT toUInt8((countIf(explain LIKE '%Optimized trivial count with sparsity filter%') > 0) = getSetting('enable_analyzer')) FROM (EXPLAIN SELECT count() FROM txn_count_snapshot WHERE s = 0 SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1)"

echo 'tx3 counts its own snapshot: the part tx1 drops still counts, the parts tx2 adds do not'
tx 3 "SELECT count() FROM txn_count_snapshot SETTINGS optimize_trivial_count_query = 1"
tx 3 "SELECT count() FROM txn_count_snapshot WHERE s = 0 SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1"

echo 'a non-transactional read observes the uncommitted work of both tx1 and tx2'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM txn_count_snapshot SETTINGS optimize_trivial_count_query = 1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM txn_count_snapshot WHERE s = 0 SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1"

tx 3 "COMMIT"
tx 1 "ROLLBACK"
tx 2 "ROLLBACK"

echo 'both transactions rolled back, the table is back to its initial three rows'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM txn_count_snapshot SETTINGS optimize_trivial_count_query = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE txn_count_snapshot"
