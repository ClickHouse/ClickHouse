#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-encrypted-storage, no-object-storage, no-parallel
#
# Regression test for classifying and retrying failures while reading a part's txn_version.txt
# during PartLoadingTree build (via ATTACH, which drives loadDataParts -> PartLoadingTree::build ->
# read_txn_status). Intersecting parts are fabricated so the intersection resolution reads the
# transaction metadata of both parts.
#
# no-parallel: the failpoints below are server-global, so a concurrent test hitting a part
# intersection could otherwise consume the injected fault.
#
# Phases:
#   1. transient error (ONCE) is retried -> ATTACH succeeds.
#   2. persistent transient error (REGULAR) exhausts retries -> ATTACH fails, but NOT as
#      CORRUPTED_DATA (the real transient error is rethrown).
#   3. legacy pre-`storing_version` metadata is still accepted (strict EOF must not reject it).
#   4. current-format metadata with trailing bytes -> rejected as CORRUPTED_DATA (strict EOF).
#   5. oversized metadata (> cap) -> rejected as CORRUPTED_DATA.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROLLED_BACK='version: 1
storing_version: 0
creation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)
creation_csn: 18446744073709551615
removal_tid: (0, 0, 00000000-0000-0000-0000-000000000000)
removal_csn: 0'
# Old format: `creation_tid:` immediately after the version header, no `storing_version` field.
LEGACY='version: 1
creation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)
creation_csn: 42
removal_tid: (0, 0, 00000000-0000-0000-0000-000000000000)
removal_csn: 0'
# Valid current-format record followed by trailing garbage.
GARBAGE="${ROLLED_BACK}
trailing garbage that must not be accepted"

TABLES="t_txn_io_1 t_txn_io_2 t_txn_io_3 t_txn_io_4 t_txn_io_5 t_txn_io_6"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_loading_tree_read_txn_status_fault" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_loading_tree_read_txn_status_persistent_fault" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_loading_tree_read_txn_status_permanent_fault" 2>/dev/null
    local t uuid disk_path data_path
    disk_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'" 2>/dev/null)
    for t in ${TABLES}; do
        uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.detached_tables WHERE database = currentDatabase() AND table = '${t}'" 2>/dev/null)
        if [ -n "${uuid}" ] && [ -n "${disk_path}" ]; then
            data_path="${disk_path}store/${uuid:0:3}/${uuid}"
            rm -rf "${data_path:?}/all_1_5_4_1" "${data_path:?}/all_5_6_1_0"
            $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${t}" 2>/dev/null
        fi
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${t}" 2>/dev/null
    done
}
trap cleanup EXIT
cleanup

# Creates ${TABLE}, detaches it permanently, and sets DATA_PATH. Fails fast if the detach
# postcondition does not hold (never modify a still-attached table's data directory).
setup()
{
    local table=$1
    $CLICKHOUSE_CLIENT -q "CREATE TABLE ${table} (x UInt32) ENGINE = MergeTree ORDER BY x"
    $CLICKHOUSE_CLIENT -q "INSERT INTO ${table} VALUES (42)"
    DATA_PATH=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '${table}'")
    $CLICKHOUSE_CLIENT -q "DETACH TABLE ${table} PERMANENTLY"
    local ok
    ok=$($CLICKHOUSE_CLIENT -q "
        SELECT (SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${table}') = 0
           AND (SELECT count() FROM system.detached_tables WHERE database = currentDatabase() AND table = '${table}' AND is_permanently) = 1")
    if [ "${ok}" != "1" ]; then echo "FAIL: ${table} not detached permanently"; exit 1; fi
    SOURCE="${DATA_PATH}/all_1_1_0"
}

# fabricate <part_name> <content>   (empty content => no txn_version.txt, i.e. non-transactional)
fabricate()
{
    local part=$1 content=$2
    cp -r "${SOURCE}" "${DATA_PATH}/${part}"
    if [ -n "${content}" ]; then
        printf '%s' "${content}" > "${DATA_PATH}/${part}/txn_version.txt.tmp"
        mv "${DATA_PATH}/${part}/txn_version.txt.tmp" "${DATA_PATH}/${part}/txn_version.txt"
    fi
}

# --- Phase 1: transient error retried, ATTACH succeeds ---
setup t_txn_io_1
fabricate all_1_5_4_1 "${ROLLED_BACK}"
fabricate all_5_6_1_0 ""
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT part_loading_tree_read_txn_status_fault"
if ! $CLICKHOUSE_CLIENT -q "ATTACH TABLE t_txn_io_1" 2>/dev/null; then echo "FAIL phase1: ATTACH threw (transient not retried)"; exit 1; fi
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_loading_tree_read_txn_status_fault"
A56=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='t_txn_io_1' AND name='all_5_6_1_0' AND active")
[ "${A56}" -eq 1 ] || { echo "FAIL phase1: all_5_6_1_0 active=${A56}"; exit 1; }
echo "phase1: OK"

# --- Phase 2: persistent transient error exhausts retries, fails but NOT as corruption ---
setup t_txn_io_2
fabricate all_1_5_4_1 "${ROLLED_BACK}"
fabricate all_5_6_1_0 ""
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT part_loading_tree_read_txn_status_persistent_fault"
ERR=$($CLICKHOUSE_CLIENT -q "ATTACH TABLE t_txn_io_2" 2>&1)
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_loading_tree_read_txn_status_persistent_fault"
if echo "${ERR}" | grep -q "CORRUPTED_DATA"; then echo "FAIL phase2: exhausted transient mis-reported as CORRUPTED_DATA"; exit 1; fi
if ! echo "${ERR}" | grep -qE "NETWORK_ERROR|transient"; then echo "FAIL phase2: expected transient error, got: ${ERR}"; exit 1; fi
echo "phase2: OK"

# --- Phase 3: legacy metadata accepted (strict EOF must not reject the legacy format) ---
setup t_txn_io_3
fabricate all_1_5_4_1 "${ROLLED_BACK}"
fabricate all_5_6_1_0 "${LEGACY}"
if ! $CLICKHOUSE_CLIENT -q "ATTACH TABLE t_txn_io_3" 2>/dev/null; then echo "FAIL phase3: legacy metadata wrongly rejected"; exit 1; fi
echo "phase3: OK"

# --- Phase 4: current-format metadata with trailing bytes rejected (strict EOF) ---
setup t_txn_io_4
fabricate all_1_5_4_1 ""
fabricate all_5_6_1_0 "${GARBAGE}"
ERR=$($CLICKHOUSE_CLIENT -q "ATTACH TABLE t_txn_io_4" 2>&1)
if ! echo "${ERR}" | grep -q "CORRUPTED_DATA"; then echo "FAIL phase4: trailing garbage not rejected, got: ${ERR}"; exit 1; fi
echo "phase4: OK"

# --- Phase 5: oversized metadata rejected ---
setup t_txn_io_5
fabricate all_1_5_4_1 ""
cp -r "${SOURCE}" "${DATA_PATH}/all_5_6_1_0"
head -c 5000 /dev/zero | tr '\0' 'x' > "${DATA_PATH}/all_5_6_1_0/txn_version.txt"
ERR=$($CLICKHOUSE_CLIENT -q "ATTACH TABLE t_txn_io_5" 2>&1)
if ! echo "${ERR}" | grep -q "CORRUPTED_DATA"; then echo "FAIL phase5: oversized metadata not rejected, got: ${ERR}"; exit 1; fi
echo "phase5: OK"

# --- Phase 6: non-retryable I/O error is propagated as-is (not retried, not corruption) ---
# The permanent failpoint is ONCE: if the code wrongly retried a non-retryable error, the one-shot
# fault would be consumed and ATTACH would succeed, so the grep below would fail the phase.
setup t_txn_io_6
fabricate all_1_5_4_1 "${ROLLED_BACK}"
fabricate all_5_6_1_0 ""
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT part_loading_tree_read_txn_status_permanent_fault"
ERR=$($CLICKHOUSE_CLIENT -q "ATTACH TABLE t_txn_io_6" 2>&1)
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_loading_tree_read_txn_status_permanent_fault"
if echo "${ERR}" | grep -q "CORRUPTED_DATA"; then echo "FAIL phase6: permanent I/O error mis-reported as CORRUPTED_DATA"; exit 1; fi
if ! echo "${ERR}" | grep -qE "CANNOT_OPEN_FILE|permanent I/O fault"; then echo "FAIL phase6: expected the original I/O error, got: ${ERR}"; exit 1; fi
echo "phase6: OK"
