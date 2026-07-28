#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage
#
# Regression test for the `contains`-branch rollback handling in `PartLoadingTree::add`.
#
# When a rolled-back tree-resident part **contains** the incoming committed peer (rather than
# just intersecting it), eviction must still fire. Otherwise the committed peer descends into
# the rolled-back ancestor's subtree, ends up loaded as `Outdated` instead of active, and is
# invisible to queries.
#
# Part insertion order in `PartLoadingTree::build` (sorted by (level, mutation) desc):
#   1. all_1_4_2_1  level=2, mut=1, blocks 1-4  rolled-back (`RolledBackCSN` in `txn_version.txt`)
#   2. all_1_2_1_0  level=1, mut=0, blocks 1-2  committed; contained in 1-4, disjoint from 3-4
#   3. all_3_4_1_0  level=1, mut=0, blocks 3-4  committed; contained in 1-4, disjoint from 1-2
#
# Without the fix the resulting tree is:
#   ROOT
#     └── all_1_4_2_1   (rolled back — becomes Outdated after `loadDataPart`)
#           ├── all_1_2_1_0  (committed but covered by rolled-back ancestor → inactive)
#           └── all_3_4_1_0  (committed but covered by rolled-back ancestor → inactive)
#
# With the fix the rolled-back ancestor is evicted on first descent and its committed
# descendants are re-parented at the root:
#   ROOT
#     ├── all_1_2_1_0  (active)
#     └── all_3_4_1_0  (active)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_plt_rb_contains"

# Part directories fabricated out-of-band below; `cleanup` removes them if the table is still
# detached, because their metadata is exactly what could have made `ATTACH TABLE` throw.
FABRICATED_PARTS="all_1_4_2_1 all_1_2_1_0 all_3_4_1_0"

cleanup()
{
    # A permanently detached table is invisible to `DROP TABLE` (`UNKNOWN_TABLE`) while its
    # name still blocks `CREATE TABLE`, so on failure paths that exit before `ATTACH TABLE`
    # succeeds the table must be re-attached before dropping.
    #
    # `cleanup` runs both from the `EXIT` trap and once at startup to recover a table leaked by
    # a previous failed attempt. The startup call happens before the in-shell `DATA_PATH` is
    # assigned, so the fabricated part directories (whose transaction metadata is what could
    # make `ATTACH TABLE` throw) are located from the detached table's own `uuid` instead:
    # the data path `<disk>/store/<uuid[0:3]>/<uuid>` is stable across restarts and is
    # available whenever the table is present in `system.detached_tables`.
    local detached_uuid disk_path data_path part
    detached_uuid=$($CLICKHOUSE_CLIENT -q "
        SELECT uuid FROM system.detached_tables
        WHERE database = currentDatabase() AND table = '${TABLE}'" 2>/dev/null)
    if [ -n "${detached_uuid}" ]; then
        disk_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'" 2>/dev/null)
        if [ -n "${disk_path}" ]; then
            data_path="${disk_path}store/${detached_uuid:0:3}/${detached_uuid}"
            for part in ${FABRICATED_PARTS}; do
                rm -rf "${data_path:?}/${part}"
            done
        fi
        $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${TABLE}" 2>/dev/null
    fi
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}" 2>/dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt32)
    ENGINE = MergeTree ORDER BY x
"

# Create a committed source part on disk that we will clone to fabricate the test parts.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (42)"

DATA_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT data_paths[1]
    FROM system.tables
    WHERE database = currentDatabase() AND name = '${TABLE}'
")

# `PERMANENTLY` keeps the table detached across server restarts (stress tests restart the
# server at arbitrary moments). With a plain `DETACH` the table is loaded again at startup
# while this script is still fabricating part directories out-of-band, and the server's
# version-metadata machinery races with the script on the same `txn_version.txt(.tmp)`
# files, leaving corrupted metadata behind that fails every subsequent server start.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${TABLE} PERMANENTLY"

# The detach client call alone is not a sufficient gate: if it fails (e.g. the connection is
# lost because a stress-test restart lands on it), the script would keep running under plain
# `bash` and mutate a table directory that may still be attached. Verify the detach
# postcondition and fail fast before touching anything under `DATA_PATH`.
DETACHED=$($CLICKHOUSE_CLIENT -q "
    SELECT (SELECT count() FROM system.tables
            WHERE database = currentDatabase() AND name = '${TABLE}') = 0
       AND (SELECT count() FROM system.detached_tables
            WHERE database = currentDatabase() AND table = '${TABLE}' AND is_permanently) = 1
")
if [ "${DETACHED}" != "1" ]; then
    echo "FAIL: table ${TABLE} is not detached permanently, refusing to modify its data directory"
    exit 1
fi

SOURCE="${DATA_PATH}/all_1_1_0"

# all_1_4_2_1: rolled-back ancestor (level 2, mut 1, blocks 1-4).
# `creation_csn = Tx::RolledBackCSN` is the rolled-back marker on disk.
# `creation_tid` must use a `local_tid` outside the reserved range (>`Tx::MaxReservedLocalTID=32`)
# so `wasInvolvedInTransaction()` returns true — otherwise `loadDataPart` treats the part as
# non-transactional, skips the `creation_csn == Tx::RolledBackCSN` check, leaves the part as
# `Active`, and never promotes the committed descendants below.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_4_2_1"
printf 'version: 1\nstoring_version: 0\ncreation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 18446744073709551615\nremoval_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\nremoval_csn: 0' \
    > "${DATA_PATH}/all_1_4_2_1/txn_version.txt.tmp"
mv "${DATA_PATH}/all_1_4_2_1/txn_version.txt.tmp" "${DATA_PATH}/all_1_4_2_1/txn_version.txt"

# all_1_2_1_0 and all_3_4_1_0: committed children, disjoint with each other and both contained
# in all_1_4_2_1.
cp -r "${SOURCE}" "${DATA_PATH}/all_1_2_1_0"
cp -r "${SOURCE}" "${DATA_PATH}/all_3_4_1_0"

# `ATTACH` triggers `loadDataParts` → `PartLoadingTree::build` → `PartLoadingTree::add`.
# Must not throw; stderr is suppressed because a `WARNING` about removing the rolled-back
# ancestor is expected on success.
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

# Both committed children must be promoted to root level and active.
check_active_count all_1_2_1_0 1
check_active_count all_3_4_1_0 1
# Rolled-back ancestor must not be active.
check_active_count all_1_4_2_1 0

echo OK
