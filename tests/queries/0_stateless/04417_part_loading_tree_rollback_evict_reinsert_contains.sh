#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage
#
# Regression test for the ordered-reinsertion contract of `PartLoadingTree::add`.
#
# When a rolled-back tree-resident part is evicted in the intersection case, its orphaned
# descendants and the incoming part are re-inserted sorted by (level, mutation) descending.
# This test exercises the case the sort exists for: one reinserted committed part *contains*
# another reinserted committed part. The container must be re-added before the part it
# contains, otherwise adding the container afterwards would hit the generic intersection
# branch (which does not handle `incoming.contains(existing)`) and throw a `LOGICAL_ERROR`.
#
# Containment per `MergeTreePartInfo::contains` requires the container to have a strictly
# higher level whenever the block ranges differ, so the (level, mutation) sort always places
# the container first — this test locks that invariant in.
#
# Part insertion order in `PartLoadingTree::build` (sorted by (level, mutation) desc):
#   1. all_1_5_4_1  level=4, mut=1, blocks 1-5  rolled-back (`RolledBackCSN` in `txn_version.txt`)
#   2. all_2_4_2_0  level=2, mut=0, blocks 2-4  committed; contained in 1-5 → child of (1); contains 2-3
#   3. all_2_3_1_0  level=1, mut=0, blocks 2-3  committed; contained in 2-4 → grandchild of (1)
#   4. all_5_6_1_0  level=1, mut=0, blocks 5-6  committed; intersects 1-5 (shares block 5) → evicts (1)
#
# Tree before eviction:
#   ROOT
#     └── all_1_5_4_1   (rolled back)
#           └── all_2_4_2_0
#                 └── all_2_3_1_0
#
# Adding all_5_6_1_0 intersects the rolled-back all_1_5_4_1, which is evicted; its descendants
# (all_2_4_2_0, all_2_3_1_0) and the incoming all_5_6_1_0 are re-inserted. The container
# all_2_4_2_0 (level 2) is re-added before all_2_3_1_0 (level 1), so the latter descends into it:
#   ROOT
#     ├── all_2_4_2_0   (active)
#     │     └── all_2_3_1_0   (covered → inactive)
#     └── all_5_6_1_0   (active)
#
# Expected outcome:
#   - `ATTACH TABLE` must succeed (no `LOGICAL_ERROR`).
#   - `all_2_4_2_0` and `all_5_6_1_0` must be active.
#   - `all_2_3_1_0` must be inactive (covered by the active container `all_2_4_2_0`).
#   - `all_1_5_4_1` must not be active.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_plt_evict_reinsert"

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

# Create a committed source part on disk that we clone to fabricate the test parts.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (42)"

DATA_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT data_paths[1]
    FROM system.tables
    WHERE database = currentDatabase() AND name = '${TABLE}'
")

$CLICKHOUSE_CLIENT -q "DETACH TABLE ${TABLE}"

SOURCE="${DATA_PATH}/all_1_1_0"

# all_1_5_4_1: rolled-back ancestor (level 4, mut 1, blocks 1-5).
# creation_csn = Tx::RolledBackCSN = 18446744073709551615 makes `read_txn_status` return
# `RolledBack` without consulting `TransactionLog`. creation_tid uses a transactional `local_tid`
# outside the reserved range (>`Tx::MaxReservedLocalTID=32`, here 33) so the TID is well-formed
# (a `local_tid` of 1 would be `Tx::NonTransactionalLocalTID` and violate the `TransactionID`
# invariant). Written atomically (`.tmp` then rename) to avoid a partial-read race.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_5_4_1"
printf 'version: 1\nstoring_version: 0\ncreation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 18446744073709551615\nremoval_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\nremoval_csn: 0' \
    > "${DATA_PATH}/all_1_5_4_1/txn_version.txt.tmp"
mv "${DATA_PATH}/all_1_5_4_1/txn_version.txt.tmp" "${DATA_PATH}/all_1_5_4_1/txn_version.txt"

# Committed (non-transactional) parts, cloned from the source part: they carry no
# `txn_version.txt`, so `read_txn_status` reports `NoMetadata`.
# all_2_4_2_0: container of all_2_3_1_0; contained in all_1_5_4_1.
cp -r "${SOURCE}" "${DATA_PATH}/all_2_4_2_0"
# all_2_3_1_0: contained in all_2_4_2_0.
cp -r "${SOURCE}" "${DATA_PATH}/all_2_3_1_0"
# all_5_6_1_0: intersects all_1_5_4_1 and triggers its eviction.
cp -r "${SOURCE}" "${DATA_PATH}/all_5_6_1_0"

# `ATTACH` triggers `loadDataParts` → `PartLoadingTree::build` → `PartLoadingTree::add`.
# Must not throw; stderr is suppressed because an INFO/WARNING about removing the rolled-back
# part is expected on success.
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

# Container re-inserted before the part it contains must be active.
check_active_count all_2_4_2_0 1
# Contained part is covered by the active container → inactive.
check_active_count all_2_3_1_0 0
# Intersecting committed part that evicted the rolled-back ancestor must be active.
check_active_count all_5_6_1_0 1
# Rolled-back ancestor must not be active.
check_active_count all_1_5_4_1 0

echo OK
