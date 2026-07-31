#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage
#
# Regression test for the tmp-only transaction-metadata rollback path in `PartLoadingTree::add`.
#
# `VersionMetadataOnDisk::loadMetadata` treats a part that has only a `txn_version.txt.tmp`
# file (and no final `txn_version.txt`) as rolled back: the creating transaction was
# interrupted before it could atomically rename the metadata into place, so it never
# committed. `read_txn_status` in `PartLoadingTree::add` must classify such a part the same
# way. Before the fix it only probed the final `txn_version.txt`, returned `NoMetadata`
# (i.e. non-transactional) for a tmp-only leftover, and an intersecting committed peer then
# fell through to the generic intersecting-parts `LOGICAL_ERROR` during `ATTACH`.
#
# Part insertion order inside `PartLoadingTree::build` (sorted by (level, mutation) desc):
#   1. all_1_2_1_0  level=1, mut=0, blocks 1-2  rolled back (only `txn_version.txt.tmp`)
#   2. all_2_3_0_0  level=0, mut=0, blocks 2-3  committed; intersects 1-2 → replaces (1)
#
# Expected outcome after `PartLoadingTree::build`:
#   - `all_1_2_1_0` is erased (rolled back).
#   - `all_2_3_0_0` is inserted as the surviving part.
#   - `ATTACH TABLE` must succeed (no `LOGICAL_ERROR`).
#   - `all_2_3_0_0` must be active; `all_1_2_1_0` must not be active.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_plt_tmp_metadata"

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

# all_1_2_1_0: rolled-back leftover with ONLY a `txn_version.txt.tmp` file (level 1, mut 0,
# blocks 1-2). The absence of the final `txn_version.txt` (never renamed into place) is what
# marks the creating transaction as interrupted before commit, i.e. rolled back. The `.tmp`
# content is deliberately a plausible in-flight record; its exact value is irrelevant because
# `loadMetadata`/`read_txn_status` decide rollback purely from the tmp-only file layout.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_2_1_0"
printf 'version: 1\nstoring_version: 0\ncreation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 0\nremoval_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\nremoval_csn: 0' \
    > "${DATA_PATH}/all_1_2_1_0/txn_version.txt.tmp"

# all_2_3_0_0: committed intersecting part (level 0, mut 0, blocks 2-3), no transaction
# metadata → non-transactional/committed. Shares block 2 with `all_1_2_1_0`, so it intersects
# without containment and triggers eviction of the rolled-back leftover.
cp -r "${SOURCE}" "${DATA_PATH}/all_2_3_0_0"

# `ATTACH` triggers `loadDataParts` → `PartLoadingTree::build` → `PartLoadingTree::add`.
# Must not throw `LOGICAL_ERROR` — verified by checking the client exit code.
# Stderr is suppressed because an INFO/WARNING log about removing the rolled-back part
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

# Committed intersecting part must be active.
check_active_count all_2_3_0_0 1
# Rolled-back tmp-only leftover must not be active.
check_active_count all_1_2_1_0 0

echo OK
