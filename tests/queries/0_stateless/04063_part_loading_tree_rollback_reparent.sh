#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage
#
# Regression test for the child re-parenting path in PartLoadingTree::add.
#
# When a rolled-back tree-resident part is replaced by an intersecting committed
# part, any children already attached to the rolled-back node must be re-inserted
# at the current tree level rather than silently dropped.
#
# Part insertion order inside PartLoadingTree::build (sorted by (level, mutation) desc):
#   1. all_1_2_2_1  level=2, mut=1, blocks 1-2  rolled-back (RolledBackCSN in txn_version.txt)
#   2. all_1_1_1_1  level=1, mut=1, blocks 1-1  committed; contained in 1-2 → child of (1)
#   3. all_2_3_1_0  level=1, mut=0, blocks 2-3  committed; intersects 1-2 → replaces (1)
#   4. all_1_1_0    level=0, mut=0, blocks 1-1  committed source (original insert)
#
# The rolled-back parent must have mutation >= child's mutation so that contains()
# returns true and all_1_1_1_1 is inserted as a child of all_1_2_2_1 before
# all_2_3_1_0 triggers the replacement. With mut=0 the check 0 >= 1 would fail and
# the orphaned-children reinsertion path would never be exercised.
#
# Expected outcome after PartLoadingTree::build:
#   - all_1_2_2_1 is erased (rolled back).
#   - all_1_1_1_1 (formerly its child) is re-inserted at the root level.
#   - all_2_3_1_0 is inserted as a sibling of all_1_1_1_1.
#   - ATTACH TABLE must not throw LOGICAL_ERROR.
#   - all_1_1_1_1 must be active (not silently absent from the loading tree).
#   - all_1_2_2_1 must not be active.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_plt_reparent"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt32)
    ENGINE = MergeTree ORDER BY x
"

# One insert creates a committed part (all_1_1_0) with valid data files that we
# will copy to build the fake parts needed for the test scenario.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (42)"

DATA_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT data_paths[1]
    FROM system.tables
    WHERE database = currentDatabase() AND name = '${TABLE}'
")

$CLICKHOUSE_CLIENT -q "DETACH TABLE ${TABLE}"

SOURCE="${DATA_PATH}/all_1_1_0"

# all_1_2_2_1: rolled-back parent (level 2, mut 1, blocks 1-2).
# creation_csn = Tx::RolledBackCSN = 18446744073709551615 causes read_txn_status
# to return RolledBack without consulting TransactionLog, making the rollback unambiguous.
# creation_tid uses start_csn=2 (not 1=PrehistoricCSN) so wasInvolvedInTransaction()
# returns true and loadDataPart also correctly handles it as a transactional part.
# The file is written atomically (write to .tmp then rename) to avoid a race where
# the server reads an empty file between truncation and write.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_2_2_1"
printf 'version: 1\ncreation_tid: (2, 1, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 18446744073709551615' \
    > "${DATA_PATH}/all_1_2_2_1/txn_version.txt.tmp"
mv "${DATA_PATH}/all_1_2_2_1/txn_version.txt.tmp" "${DATA_PATH}/all_1_2_2_1/txn_version.txt"

# all_1_1_1_1: committed child of the rolled-back parent (level 1, mut 1, blocks 1-1).
# Sorting places it second; blocks 1-1 are contained within 1-2, so it becomes a
# child of all_1_2_2_1 before all_2_3_1_0 triggers the replacement.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_1_1_1"

# all_2_3_1_0: committed intersecting part (level 1, mut 0, blocks 2-3).
# Intersects all_1_2_2_1 (shares block 2); triggers the replacement and the
# orphaned-children reinsertion code path.
cp -r "${SOURCE}" "${DATA_PATH}/all_2_3_1_0"

# ATTACH triggers loadDataParts -> PartLoadingTree::build -> PartLoadingTree::add.
# Must not throw LOGICAL_ERROR.
# Redirect stderr to /dev/null: the expected WARNING log about removing the
# rolled-back part would otherwise fail the test harness' stderr check.
# Correctness is verified by the queries below.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${TABLE}" 2>/dev/null

# Verify all_1_1_1_1 (the re-parented child) is active.
$CLICKHOUSE_CLIENT -q "
    SELECT name, active
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = '${TABLE}'
      AND name = 'all_1_1_1_1'
"

# Verify the rolled-back parent is not active.
ACTIVE_PARENT=$($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM system.parts
    WHERE database = currentDatabase()
      AND table = '${TABLE}'
      AND name = 'all_1_2_2_1'
      AND active
")
if [ "${ACTIVE_PARENT}" -ne 0 ]; then
    echo "FAIL: rolled-back part all_1_2_2_1 is unexpectedly active"
else
    echo "OK"
fi
