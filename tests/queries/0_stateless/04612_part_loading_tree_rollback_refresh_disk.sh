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
# containment topology that meant a readonly refresh:
#   - buried the committed children under the Outdated rolled-back ancestor, and
#   - re-activated the rolled-back ancestor itself.
#
# Topology (== 04241_part_loading_tree_rollback_contains), but the parts appear AFTER the
# readonly reader is loaded, so they are surfaced by refreshDataPartsOnce, not by startup:
#   all_1_4_2_1  rolled-back ancestor  (blocks 1-4, level 2)
#   all_1_2_1_0  committed child       (blocks 1-2, contained, disjoint from 3-4)
#   all_3_4_1_0  committed child       (blocks 3-4, contained, disjoint from 1-2)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WRITER="writer_${CLICKHOUSE_DATABASE}"
READER="reader_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${WRITER} SYNC" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${READER} SYNC" 2>/dev/null
}
trap cleanup EXIT
cleanup

# A writer on a read-write plain_rewritable disk and a reader on a read-only disk over the
# SAME backing path (plain_rewritable stores files at a local path; the two disks may share
# it). The reader has all-read-only disks, so SYSTEM RESTART DISK re-scans it via
# refreshDataPartsOnce. `no-object-storage` is deliberately NOT set: this test REQUIRES a
# local backing path (object_storage_type = local) so we can fabricate the rolled-back part.
DISK_PATH="disks/04612/${CLICKHOUSE_DATABASE}/"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${WRITER} (x UInt32) ORDER BY x
    SETTINGS table_disk = true,
      disk = disk(
          name = 04612_writer_${CLICKHOUSE_DATABASE},
          type = object_storage, object_storage_type = local,
          metadata_type = plain_rewritable, path = '${DISK_PATH}')
"

# Committed source part we clone to fabricate the test parts.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${WRITER} VALUES (42)"

# Physical directory backing the plain_rewritable disk (relative to the server data path).
STORE=$($CLICKHOUSE_CLIENT -q "
    SELECT concatWithSeparator('/', trimRight(path, '/'), '${DISK_PATH}')
    FROM system.disks WHERE name = '04612_writer_${CLICKHOUSE_DATABASE}'
")
# Fallbacks: some servers report the fully-resolved path already.
[ -d "${STORE}" ] || STORE=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = '04612_writer_${CLICKHOUSE_DATABASE}'")
if [ ! -d "${STORE}" ]; then
    echo "FAIL: could not locate plain_rewritable backing dir (${STORE})"
    exit 1
fi

# plain_rewritable maps a logical part name to a random directory via __meta/<rnd>/prefix.path.
# Find the random dir for the committed source part all_1_1_0.
SRC_RND=""
for meta in "${STORE}"__meta/*/prefix.path "${STORE}"/__meta/*/prefix.path; do
    [ -f "${meta}" ] || continue
    if [ "$(cat "${meta}")" = "all_1_1_0/" ]; then
        SRC_RND=$(basename "$(dirname "${meta}")")
        META_DIR=$(dirname "$(dirname "${meta}")")
        STORE=$(dirname "${META_DIR}")/
        break
    fi
done
if [ -z "${SRC_RND}" ]; then
    echo "FAIL: could not find the random dir mapped to all_1_1_0 under ${STORE}"
    exit 1
fi

# Stage clones of the source part under fresh random names mapped to the fabricated logical
# names. Staged into a holding area so we can inject them only AFTER the reader has loaded.
STAGE=$(mktemp -d)
declare -a RND_NAMES
stage_part()
{
    local logical=$1 rolled_back=$2
    local rnd; rnd=$(tr -dc 'a-z' < /dev/urandom | head -c 32)
    cp -r "${STORE}${SRC_RND}" "${STAGE}/${rnd}"
    mkdir -p "${STAGE}/meta_${rnd}"
    printf '%s/' "${logical}" > "${STAGE}/meta_${rnd}/prefix.path"
    if [ "${rolled_back}" = "1" ]; then
        # `creation_csn = Tx::RolledBackCSN` marks the part rolled back on disk. `creation_tid`
        # uses a local_tid > Tx::MaxReservedLocalTID (32) so wasInvolvedInTransaction() is true.
        printf 'version: 1\nstoring_version: 0\ncreation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)\ncreation_csn: 18446744073709551615\nremoval_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\nremoval_csn: 0' \
            > "${STAGE}/${rnd}/txn_version.txt"
    fi
    RND_NAMES+=("${rnd}")
}

stage_part all_1_4_2_1 1
stage_part all_1_2_1_0 0
stage_part all_3_4_1_0 0

# Detach the writer and drop the source part so the readonly reader loads an EMPTY part set.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${WRITER}"
rm -rf "${STORE}${SRC_RND}" "${STORE}__meta/${SRC_RND}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${READER} (x UInt32) ORDER BY x
    SETTINGS table_disk = true,
      disk = disk(
          readonly = true,
          name = 04612_reader_${CLICKHOUSE_DATABASE},
          type = object_storage, object_storage_type = local,
          metadata_type = plain_rewritable, path = '${DISK_PATH}')
"

# Inject the fabricated topology into the shared store (reader already loaded empty).
for rnd in "${RND_NAMES[@]}"; do
    mv "${STAGE}/${rnd}" "${STORE}${rnd}"
    mkdir -p "${STORE}__meta/${rnd}"
    mv "${STAGE}/meta_${rnd}/prefix.path" "${STORE}__meta/${rnd}/prefix.path"
done
rm -rf "${STAGE}"

# Trigger refreshDataPartsOnce for the readonly reader.
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART DISK 04612_reader_${CLICKHOUSE_DATABASE}"

check_active_count()
{
    local part_name=$1 expected=$2 actual
    actual=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = '${READER}'
          AND name = '${part_name}' AND active
    ")
    if [ "${actual}" -ne "${expected}" ]; then
        echo "FAIL: part ${part_name} active count is ${actual}, expected ${expected}"
        exit 1
    fi
}

# Both committed children must be promoted to root level and active after the refresh.
check_active_count all_1_2_1_0 1
check_active_count all_3_4_1_0 1
# The rolled-back ancestor must not be re-activated.
check_active_count all_1_4_2_1 0

echo OK
