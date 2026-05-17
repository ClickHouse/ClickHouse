#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage
#
# Regression test for the child re-parenting path in `PartLoadingTree::add`.
#
# When a rolled-back tree-resident part is replaced by an intersecting committed
# part, any children already attached to the rolled-back node must be re-inserted
# at the current tree level rather than silently dropped.
#
# Part insertion order inside `PartLoadingTree::build` (sorted by (level, mutation) desc):
#   1. all_1_2_2_1  level=2, mut=1, blocks 1-2  rolled-back (`RolledBackCSN` in `txn_version.txt`)
#   2. all_1_1_1_1  level=1, mut=1, blocks 1-1  committed; contained in 1-2 → child of (1)
#   3. all_2_3_1_0  level=1, mut=0, blocks 2-3  committed; intersects 1-2 → replaces (1)
#   4. all_1_1_0    level=0, mut=0, blocks 1-1  committed source (original insert)
#
# The rolled-back parent must have mutation >= child's mutation so that `contains`
# returns true and `all_1_1_1_1` is inserted as a child of `all_1_2_2_1` before
# `all_2_3_1_0` triggers the replacement. With mut=0 the check 0 >= 1 would fail and
# the orphaned-children reinsertion path would never be exercised.
#
# Expected outcome after `PartLoadingTree::build`:
#   - `all_1_2_2_1` is erased (rolled back).
#   - `all_1_1_1_1` (formerly its child) is re-inserted at the root level.
#   - `all_2_3_1_0` is inserted as a sibling of `all_1_1_1_1`.
#   - `ATTACH TABLE` must succeed (no `LOGICAL_ERROR`).
#   - `all_1_1_1_1` and `all_2_3_1_0` must be active.
#   - `all_1_2_2_1` must not be active.

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

# One insert creates a committed part (`all_1_1_0`) with valid data files that we
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
# Multi-line format expected by `VersionInfo::readFromMultiLineBuffer`:
# `storing_version` is required (without it, the old-format fallback overrides
# `creation_csn` with `Tx::NonTransactionalCSN` and the rollback is silently lost).
# creation_csn = Tx::RolledBackCSN = 18446744073709551615 causes `read_txn_status`
# to return `RolledBack` without consulting `TransactionLog`, making the rollback unambiguous.
# creation_tid uses start_csn=2 (not 1=NonTransactionalCSN) so the TID is treated as
# transactional. The file is written atomically (write to `.tmp` then rename) to avoid
# a race where the server reads a partial file.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_2_2_1"
printf 'version: 1\nstoring_version: 0\ncreation_tid: (2, 1, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 18446744073709551615\nremoval_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\nremoval_csn: 0' \
    > "${DATA_PATH}/all_1_2_2_1/txn_version.txt.tmp"
mv "${DATA_PATH}/all_1_2_2_1/txn_version.txt.tmp" "${DATA_PATH}/all_1_2_2_1/txn_version.txt"

# all_1_1_1_1: committed child of the rolled-back parent (level 1, mut 1, blocks 1-1).
# Sorting places it second; blocks 1-1 are contained within 1-2, so it becomes a
# child of `all_1_2_2_1` before `all_2_3_1_0` triggers the replacement.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_1_1_1"

# all_2_3_1_0: committed intersecting part (level 1, mut 0, blocks 2-3).
# Intersects `all_1_2_2_1` (shares block 2); triggers the replacement and the
# orphaned-children reinsertion code path.
cp -r "${SOURCE}" "${DATA_PATH}/all_2_3_1_0"

# `ATTACH` triggers `loadDataParts` → `PartLoadingTree::build` → `PartLoadingTree::add`.
# Must not throw `LOGICAL_ERROR` — verified by checking the client exit code.
# Stderr is suppressed because a `WARNING` log about removing the rolled-back part
# is expected on success and would otherwise fail the test harness' stderr check.
if ! $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${TABLE}" 2>/dev/null; then
    echo "FAIL: ATTACH TABLE threw an exception"
    exit 1
fi

check_active_count()
{
    local part_name=$1
    local expected=$2
    local actual
    actual=$($CLICKHOUSE_CLIENT -q "
        SELECT count()
        FROM system.parts
        WHERE database = currentDatabase()
          AND table = '${TABLE}'
          AND name = '${part_name}'
          AND active
    ")
    if [ "${actual}" -ne "${expected}" ]; then
        echo "FAIL: part ${part_name} active count is ${actual}, expected ${expected}"
        exit 1
    fi
}

# Re-parented committed child must be active.
check_active_count all_1_1_1_1 1
# Rolled-back parent must not be active.
check_active_count all_1_2_2_1 0
# Committed intersecting part must be active.
check_active_count all_2_3_1_0 1

echo OK
