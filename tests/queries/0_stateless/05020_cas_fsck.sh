#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# `SYSTEM CAS FSCK <disk>` (runs on a RUNNING disk, T13) + GC RUN's `pending_*` drain
# columns + the fail-closed FORGET teardown (spec rev.8 §5/§9). FSCK is a read-only reachability audit
# that now runs directly on the mounted, live disk and prints a clean one-row summary. The GC RUN result
# set carries the retire pipeline's REMAINING (not this-round-delta) `pending_*` columns; on a disk with
# nothing outstanding to reclaim they read 0. Teardown is fail-closed: DROP the table, `SYSTEM CONTENT
# ADDRESSED FORGET` the disk (force-Vanish, node-local), verify via system.cas_mounts that
# it reads exactly `vanished(forgotten)`, and only THEN `rm -rf` the pool dir (FORGET stopped and joined
# every CAS background thread for this disk). A failed FORGET or an unexpected lifecycle aborts the test
# with the pool dir left in place (the scripts have no `set -e`, so the checks are explicit).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_NAME="ca_fsck_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
POOL_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}_fsck_${RANDOM}"
rm -rf "${POOL_DIR:?}"
mkdir -p "${POOL_DIR}"
DISK_CA="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05020',
    name = '${DISK_NAME}',
    path = '${POOL_DIR}/')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_fsck SYNC"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_fsck (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS disk = ${DISK_CA}"

# --- GC RUN's result set carries the new pending_* columns while the disk is mounted, and they read 0
#     on this fresh pool (nothing was ever written, so nothing was ever condemned) ---
${CLICKHOUSE_CLIENT} --format TSVWithNames --query "SYSTEM CAS GC RUN '${DISK_NAME}'" \
    | tr '\t' '\n' | grep -c "pending_candidates\|pending_condemned\|pending_retired"
${CLICKHOUSE_CLIENT} --format TSV --query "SYSTEM CAS GC RUN '${DISK_NAME}'" \
    | awk -F'\t' '{print $(NF-2), $(NF-1), $NF}'

# --- FSCK on the RUNNING, healthy pool (T13: FSCK runs on a mounted disk): a clean one-row summary,
#     no dangling/unreachable ---
${CLICKHOUSE_CLIENT} --format TSVWithNames --query "SYSTEM CAS FSCK '${DISK_NAME}'" \
    | sed "s/${DISK_NAME}/<disk>/"

# --- A non-CA disk is rejected (the always-present local \`default\`) ---
echo -n 'fsck_non_ca_disk_rejected: '
${CLICKHOUSE_CLIENT} --query "SYSTEM CAS FSCK default" 2>&1 \
    | grep -cm1 "is not a content-addressed disk"

# --- FSCK requires an explicit disk (syntax error) ---
echo -n 'fsck_requires_disk: '
${CLICKHOUSE_CLIENT} --query "SYSTEM CAS FSCK" 2>&1 \
    | grep -cm1 "Syntax error"

# --- Fail-closed teardown (spec rev.8 §5/§9): DROP the table, FORGET the disk (force-Vanish, node-local),
#     verify it reads exactly `vanished(forgotten)`, and only then rm. A failed FORGET or an unexpected
#     lifecycle aborts here, leaving the pool dir in place. ---
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_fsck SYNC"
# FORGET logs an operator WARNING (the decommission is deliberately prominent in the server log); the
# clickhouse-test harness runs the client at --send_logs_level=warning, which would stream that expected
# warning to stderr and be flagged as a failure. Suppress it on the client for the FORGET call only.
${CLICKHOUSE_CLIENT} --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${DISK_NAME}'" || {
    echo "FORGET failed — leaving pool dir in place (fail-closed)"; exit 1; }
LIFECYCLE=$(${CLICKHOUSE_CLIENT} --query "
    SELECT lifecycle || '(' || lifecycle_reason || ')' FROM system.cas_mounts
    WHERE disk = '${DISK_NAME}'")
[ "${LIFECYCLE}" = "vanished(forgotten)" ] || {
    echo "unexpected lifecycle after FORGET: ${LIFECYCLE}"; exit 1; }

# --- A second FORGET is idempotent: it succeeds and the disk stays `vanished(forgotten)` (an already
#     terminal Vanished pool is the terminal truth — nothing to force, nothing to double-retire). ---
${CLICKHOUSE_CLIENT} --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${DISK_NAME}'" || {
    echo "second FORGET failed — leaving pool dir in place (fail-closed)"; exit 1; }
LIFECYCLE_AGAIN=$(${CLICKHOUSE_CLIENT} --query "
    SELECT lifecycle || '(' || lifecycle_reason || ')' FROM system.cas_mounts
    WHERE disk = '${DISK_NAME}'")
echo "second_forget_idempotent: ${LIFECYCLE_AGAIN}"

rm -rf "${POOL_DIR:?}"   # safe: FORGET stopped and joined every CAS thread for this disk
