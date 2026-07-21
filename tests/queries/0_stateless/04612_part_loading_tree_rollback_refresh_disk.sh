#!/usr/bin/env bash
# Tags: no-random-settings, no-replicated-database, no-shared-merge-tree, no-fasttest
# Tag no-random-settings: the test fabricates exact on-disk part metadata (a raw
#   txn_version.txt and cloned part directories), matching the sibling plain_rewritable
#   read-only refresh tests 04318/04421/03362.
# Tag no-replicated-database: plain rewritable should not be shared between replicas.
# Tag no-fasttest: uses an object_storage=local (plain_rewritable) disk and Keeper-backed
#   transaction metadata, neither available in the fast test.
#
# Regression for the `refreshDataPartsOnce` orphan-promotion gap (follow-up to #100992).
#
# #100992 taught the startup part loader (`loadDataPartsFromDisk`) to promote committed
# descendants of a rolled-back top-level part. The read-only refresh path
# (`refreshDataPartsOnce`, used by the background refresh task and `SYSTEM RESTART DISK`)
# had no such promotion and unconditionally committed every top-level node. On the 04241
# containment topology that meant a readonly refresh buried the committed children under
# the Outdated rolled-back ancestor and re-activated the rolled-back ancestor itself.
#
# Topology (== 04241_part_loading_tree_rollback_contains), but the parts appear AFTER the
# readonly reader is loaded, so they are surfaced by refreshDataPartsOnce, not by startup:
#   all_1_4_2_1  rolled-back ancestor  (blocks 1-4, level 2)
#   all_1_2_1_0  committed child       (blocks 1-2, contained, disjoint from 3-4)
#   all_3_4_1_0  committed child       (blocks 3-4, contained, disjoint from 1-2)
#
# Two phases, one per code path:
#   Phase 1 (same pass): all three parts appear before one refresh. The in-loop promotion
#     skips committing the rolled-back top-level node and re-enqueues its children.
#   Phase 2 (cross refresh): the rolled-back ancestor appears first and is indexed Outdated
#     by refresh N. On a read-only table the old-part cleanup never runs, so it stays
#     indexed; the committed children appear only before refresh N+1. The top-level seed
#     must descend past the already-indexed non-active ancestor to surface them, otherwise
#     they stay invisible until a full restart/reattach.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WRITER="writer_${CLICKHOUSE_DATABASE}"
READER1="reader1_${CLICKHOUSE_DATABASE}"
READER2="reader2_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${WRITER} SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${READER1} SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${READER2} SYNC" 2>/dev/null
    rm -rf "${DISK_ROOT:-}" "${STAGE:-}" "${CLONE_SRC:-}"
}
trap cleanup EXIT
cleanup

# A writer on a read-write plain_rewritable disk creates the source part and the disk layout
# (format_version.txt + the __meta mapping). Each phase gets its own copy of that layout so
# the two readers do not share a __meta namespace. The readers have all-read-only disks, so
# SYSTEM RESTART DISK re-scans them via refreshDataPartsOnce. `no-object-storage` is
# deliberately NOT set: the test REQUIRES a local backing path (object_storage_type = local)
# so it can fabricate the rolled-back part on disk.
#
# The disk backing path must be ABSOLUTE. An object_storage=local disk resolves a relative
# `path` against the server's working directory, which differs from this script's directory
# under the CI runner, so a relative path would be unreachable from here. CLICKHOUSE_TMP is an
# absolute, test-unique directory both the server and this script can reach.
DISK_ROOT="${CLICKHOUSE_TMP:?}/04612_${CLICKHOUSE_DATABASE}"
rm -rf "${DISK_ROOT}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${WRITER} (x UInt32) ORDER BY x
    SETTINGS table_disk = true,
      disk = disk(
          name = 04612_writer_${CLICKHOUSE_DATABASE},
          type = object_storage, object_storage_type = local,
          metadata_type = plain_rewritable, path = '${DISK_ROOT}/w/')
"

# Committed source part we clone to fabricate the test parts.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${WRITER} VALUES (42)"

# data_paths[1] is the absolute backing directory of the writer's disk.
STORE=$($CLICKHOUSE_CLIENT -q "
    SELECT data_paths[1] FROM system.tables
    WHERE database = currentDatabase() AND name = '${WRITER}'
")
STORE="${STORE%/}/"
if [ ! -d "${STORE}" ]; then
    echo "FAIL: could not locate plain_rewritable backing dir (${STORE})"
    exit 1
fi

# plain_rewritable maps a logical part name to a random directory via __meta/<rnd>/prefix.path.
# Find the random dir for the committed source part all_1_1_0.
SRC_RND=""
for meta in "${STORE}"__meta/*/prefix.path; do
    [ -f "${meta}" ] || continue
    if [ "$(cat "${meta}")" = "all_1_1_0/" ]; then
        SRC_RND=$(basename "$(dirname "${meta}")")
        break
    fi
done
if [ -z "${SRC_RND}" ]; then
    echo "FAIL: could not find the random dir mapped to all_1_1_0 under ${STORE}"
    exit 1
fi

# Keep a copy of the source part, then detach the writer. Each phase's reader is created on
# a fresh empty path (the CREATE initializes the plain_rewritable format itself), and the
# fabricated parts are injected only afterwards.
CLONE_SRC=$(mktemp -d); cp -r "${STORE}${SRC_RND}/." "${CLONE_SRC}/"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${WRITER}"

STAGE=$(mktemp -d)

# Fabricate part <logical> under a fresh random dir in ${STAGE}, mapped via prefix.path.
# rolled_back=1 writes a raw txn_version.txt with creation_csn = Tx::RolledBackCSN and a
# creation_tid whose local_tid > Tx::MaxReservedLocalTID (32) so wasInvolvedInTransaction()
# is true. Prints the random dir name.
stage_part()
{
    local logical=$1 rolled_back=$2 rnd
    rnd=$(tr -dc 'a-z' < /dev/urandom | head -c 32)
    cp -r "${CLONE_SRC}" "${STAGE}/${rnd}"
    mkdir -p "${STAGE}/meta_${rnd}"
    printf '%s/' "${logical}" > "${STAGE}/meta_${rnd}/prefix.path"
    if [ "${rolled_back}" = "1" ]; then
        printf 'version: 1\nstoring_version: 0\ncreation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 18446744073709551615\nremoval_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\nremoval_csn: 0' \
            > "${STAGE}/${rnd}/txn_version.txt"
    fi
    echo "${rnd}"
}

# Move a staged part (its random dir + prefix.path) into a reader's store.
inject_part()
{
    local store=$1 rnd=$2
    mv "${STAGE}/${rnd}" "${store}${rnd}"
    mkdir -p "${store}__meta/${rnd}"
    mv "${STAGE}/meta_${rnd}/prefix.path" "${store}__meta/${rnd}/prefix.path"
}

# Create a readonly reader over a fresh, empty plain_rewritable layout at ${DISK_ROOT}/<sub>.
# Prints the reader's absolute store directory (from data_paths[1]).
make_reader()
{
    # Separate `local` statements: a single `local a=$2 b="${a}"` evaluates every RHS
    # before assigning, so `${a}` would still be empty when building `b`.
    local table=$1
    local sub=$2
    local disk="04612_${sub}_${CLICKHOUSE_DATABASE}"
    local store
    mkdir -p "${DISK_ROOT}/${sub}/__meta"
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE ${table} (x UInt32) ORDER BY x
        SETTINGS table_disk = true,
          disk = disk(
              readonly = true, name = ${disk},
              type = object_storage, object_storage_type = local,
              metadata_type = plain_rewritable, path = '${DISK_ROOT}/${sub}/')
    "
    store=$($CLICKHOUSE_CLIENT -q "
        SELECT data_paths[1] FROM system.tables
        WHERE database = currentDatabase() AND name = '${table}'
    ")
    echo "${store%/}/"
}

check_active_count()
{
    local table=$1 part_name=$2 expected=$3 actual
    actual=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = '${table}'
          AND name = '${part_name}' AND active
    ")
    if [ "${actual}" -ne "${expected}" ]; then
        echo "FAIL: [${table}] part ${part_name} active count is ${actual}, expected ${expected}"
        exit 1
    fi
}

# ---- Phase 1: same-pass containment (in-loop orphan promotion) ----
P1_ANCESTOR=$(stage_part all_1_4_2_1 1)
P1_CHILD_A=$(stage_part all_1_2_1_0 0)
P1_CHILD_B=$(stage_part all_3_4_1_0 0)
STORE1=$(make_reader "${READER1}" r1)
inject_part "${STORE1}" "${P1_ANCESTOR}"
inject_part "${STORE1}" "${P1_CHILD_A}"
inject_part "${STORE1}" "${P1_CHILD_B}"
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART DISK 04612_r1_${CLICKHOUSE_DATABASE}"
# Both committed children promoted and active; rolled-back ancestor not re-activated.
check_active_count "${READER1}" all_1_2_1_0 1
check_active_count "${READER1}" all_3_4_1_0 1
check_active_count "${READER1}" all_1_4_2_1 0

# ---- Phase 2: cross-refresh containment (seed descends past a stale ancestor) ----
P2_ANCESTOR=$(stage_part all_1_4_2_1 1)
P2_CHILD_A=$(stage_part all_1_2_1_0 0)
P2_CHILD_B=$(stage_part all_3_4_1_0 0)
STORE2=$(make_reader "${READER2}" r2)
# Refresh N: only the rolled-back ancestor is on disk. It gets indexed but must not be active.
inject_part "${STORE2}" "${P2_ANCESTOR}"
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART DISK 04612_r2_${CLICKHOUSE_DATABASE}"
check_active_count "${READER2}" all_1_4_2_1 0
# Refresh N+1: the committed children appear only now and must be surfaced despite the
# already-indexed ancestor.
inject_part "${STORE2}" "${P2_CHILD_A}"
inject_part "${STORE2}" "${P2_CHILD_B}"
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART DISK 04612_r2_${CLICKHOUSE_DATABASE}"
check_active_count "${READER2}" all_1_2_1_0 1
check_active_count "${READER2}" all_3_4_1_0 1
check_active_count "${READER2}" all_1_4_2_1 0

rm -rf "${STAGE}" "${CLONE_SRC}" "${DISK_ROOT}"
echo OK
