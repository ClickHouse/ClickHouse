#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type (keep it off the minimal fasttest image); this test uses its
#   own unique local-object-storage pool and a per-run CAS disk name, so unlike 04290_cas_no_leftovers
#   it does not need no-parallel.

# FINDING #2 regression test: `DROP TABLE ... SYNC` on a content-addressed MergeTree used to leave the
# table's CAS ref-catalog row `live` forever whenever `DirShape::TableDir`'s `existsDirectory` observed
# zero committed refs -- an empty table, or one whose last part was just removed. `dropAllData`'s own
# `existsDirectory` precheck skipped `removeRecursive`/`dropNamespace` entirely in that shape, so the
# SQL-level drop completed normally while the CAS catalog row leaked, one per create/drop cycle.
#
# The primary oracle is the pool's OWN plain-text `cas/ref_catalog` object, read directly off disk: the
# exact `st` (lifecycle) field recorded for the table's logical namespace. `SYSTEM CAS FSCK`'s
# unreachable/dangling counts are a secondary check only -- fsck correctly regards a `live` leak as
# CONSISTENT (nothing is unreachable; the row simply never dies), so it cannot detect this defect on its
# own; `04290_cas_no_leftovers.sh`'s fsck-only oracle is exactly why FINDING #2 shipped unnoticed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POOL_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}_05023_${RANDOM}"
DISK_NAME="ca_05023_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
SERVER_ROOT_ID="dropns05023"

rm -rf "${POOL_DIR:?}"
mkdir -p "${POOL_DIR}"

CATALOG_FILE="${POOL_DIR}/ca/cas/ref_catalog"

# The pool's own plain-text catalog line for namespace $1, or empty if the namespace has no row at all.
catalog_line() {
    grep -F "\"ns\":\"$1\"" "${CATALOG_FILE}" 2>/dev/null || true
}

# The `st` (lifecycle) word recorded for namespace $1: "live"/"creating"/"removing", or "absent" if the
# namespace has no catalog row (matches `04290`'s field-by-name discipline: never assume a position).
catalog_state() {
    local line
    line=$(catalog_line "$1")
    if [ -z "${line}" ]; then
        echo "absent"
        return
    fi
    echo "${line}" | grep -o '"st":"[a-z]*"' | head -1 | sed -E 's/"st":"([a-z]*)"/\1/'
}

# ClickHouse's own store/<u3>/<uuid> fanout with the CAS archive boundary marker, exactly as
# `Cas::mirroredArchiveNamespace` builds it -- see PartPathParser.cpp. `$1` is the table's UUID.
namespace_of() {
    local uuid="$1"
    echo "${SERVER_ROOT_ID}/store/${uuid:0:3}/${uuid}@cas@"
}

DISK_DEF="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${SERVER_ROOT_ID}',
    name = '${DISK_NAME}',
    path = '${POOL_DIR}/',
    cas_gc_enabled = 1,
    cas_gc_interval_sec = 100000)"
# ^ gc_enabled=1 so `SYSTEM CAS GC RUN` is available; the interval is long enough that no background
#   round can fire during the test's own window, so the post-drop catalog state read directly below is
#   stable -- only the manual `GC RUN` loop at the end may advance it.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dropns_empty SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dropns_one_part SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dropns_negative_control SYNC"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dropns_cycle SYNC"

# ---- (1) an EMPTY table: zero parts, zero namespace files beyond format_version.txt ----
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dropns_empty (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = ${DISK_DEF}"

EMPTY_UUID=$($CLICKHOUSE_CLIENT --query "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_dropns_empty'")
EMPTY_NS=$(namespace_of "${EMPTY_UUID}")

echo "empty_table_state_before_drop $(catalog_state "${EMPTY_NS}")"

# Its only payload is the namespace-level format_version.txt: no ref stream object anywhere yet (a
# files-only life never touched by a ref op has no `_log`/`_snap` at all).
FORMAT_VERSION_HITS=$(find "${POOL_DIR}/ca/cas/ns/state" -path '*_files/format_version.txt' 2>/dev/null | wc -l)
STREAM_HITS_BEFORE=$(find "${POOL_DIR}/ca/cas/ns/stream" -type f 2>/dev/null | wc -l)
echo "empty_table_has_format_version $([ "${FORMAT_VERSION_HITS}" -ge 1 ] && echo 1 || echo 0)"
echo "empty_table_has_no_ref_stream_before_drop $([ "${STREAM_HITS_BEFORE}" -eq 0 ] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dropns_empty SYNC"

# The current branch fails here by leaving st:"live"; the fix must show "removing" (a terminal stream
# record now exists but the catalog row itself is not deleted until GC folds and reclaims it).
echo "empty_table_state_after_sync_drop $(catalog_state "${EMPTY_NS}")"
STREAM_HITS_AFTER=$(find "${POOL_DIR}/ca/cas/ns/stream" -type f 2>/dev/null | wc -l)
echo "empty_table_terminal_stream_record_exists $([ "${STREAM_HITS_AFTER}" -ge 1 ] && echo 1 || echo 0)"

# ---- (2) same shape, but with one committed part: "parts removed first, files remain" is a separate
#      path from the zero-part path above; pin it too. ----
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dropns_one_part (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = ${DISK_DEF}"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_dropns_one_part VALUES (1)"

ONE_PART_UUID=$($CLICKHOUSE_CLIENT --query "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_dropns_one_part'")
ONE_PART_NS=$(namespace_of "${ONE_PART_UUID}")
echo "one_part_table_state_before_drop $(catalog_state "${ONE_PART_NS}")"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dropns_one_part SYNC"
echo "one_part_table_state_after_sync_drop $(catalog_state "${ONE_PART_NS}")"

# ---- (3) negative control: removing the ONLY part while keeping the table must never admit removal. ----
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_dropns_negative_control (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = ${DISK_DEF}"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_dropns_negative_control VALUES (1)"

NEGATIVE_UUID=$($CLICKHOUSE_CLIENT --query "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_dropns_negative_control'")
NEGATIVE_NS=$(namespace_of "${NEGATIVE_UUID}")

$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE t_dropns_negative_control"
echo "negative_control_state_after_truncate $(catalog_state "${NEGATIVE_NS}")"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_dropns_negative_control VALUES (2)"
echo "negative_control_state_after_reinsert $(catalog_state "${NEGATIVE_NS}")"
echo "negative_control_rows $($CLICKHOUSE_CLIENT --query "SELECT count() FROM t_dropns_negative_control")"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dropns_negative_control SYNC"

# ---- (4) several same-SQL-name create/drop cycles: every fresh Atomic UUID gets its OWN namespace, so
#      a stale predecessor cannot mask a fresh leak, and a same-name CREATE must never wait on the old
#      UUID's GC. ----
CYCLE_NS_LIST=()
CYCLE_LEAK_COUNT=0
for i in 1 2 3; do
    $CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_dropns_cycle (a UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS disk = ${DISK_DEF}"
    CYCLE_UUID=$($CLICKHOUSE_CLIENT --query "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_dropns_cycle'")
    CYCLE_NS=$(namespace_of "${CYCLE_UUID}")
    CYCLE_NS_LIST+=("${CYCLE_NS}")

    $CLICKHOUSE_CLIENT --query "DROP TABLE t_dropns_cycle SYNC"
    if [ "$(catalog_state "${CYCLE_NS}")" = "live" ]; then
        CYCLE_LEAK_COUNT=$((CYCLE_LEAK_COUNT + 1))
    fi
done
echo "cycle_leaks_after_sync_drop ${CYCLE_LEAK_COUNT}"

# ---- (5) drive manual GC to a bounded fixpoint (Task 7 `pending_*` gauges, not a fixed sleep), then
#      assert every captured namespace's catalog row is gone. ----
ALL_NS=("${EMPTY_NS}" "${ONE_PART_NS}" "${CYCLE_NS_LIST[@]}")

PENDING=1
for _ in $(seq 1 60); do
    PENDING=$($CLICKHOUSE_CLIENT --query "SYSTEM CAS GC RUN '${DISK_NAME}'" --format TSVWithNames \
        | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                      { print $col["pending_condemned"] }')
    [ "${PENDING}" = "0" ] && break
    sleep 0.5
done
if [ "${PENDING}" != "0" ]; then
    echo "FAIL: GC did not drain within the bounded loop (pending=${PENDING})" >&2
    exit 1
fi

ROWS_STILL_PRESENT=0
for ns in "${ALL_NS[@]}"; do
    if [ "$(catalog_state "${ns}")" != "absent" ]; then
        ROWS_STILL_PRESENT=$((ROWS_STILL_PRESENT + 1))
    fi
done
echo "captured_rows_absent_after_gc_fixpoint $([ "${ROWS_STILL_PRESENT}" -eq 0 ] && echo 1 || echo 0)"

# ---- (6) SYSTEM CAS FSCK: secondary check only -- a `live` leak alone would read as CONSISTENT here,
#      which is exactly why the fsck-only oracle in 04290_cas_no_leftovers.sh did not catch this defect. ----
$CLICKHOUSE_CLIENT --query "SYSTEM CAS FSCK '${DISK_NAME}'" --format TSVWithNames \
    | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                  { print "fsck_unreachable", $col["unreachable"]; print "fsck_dangling", $col["dangling"] }'

# ---- (7) fail-closed teardown (spec rev.8 §5/§9): FORGET the disk (all tables already dropped above),
#      verify it, only then rm. ----
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${DISK_NAME}'" || {
    echo "FORGET failed — leaving pool dir in place (fail-closed)"; exit 1; }
LIFECYCLE=$($CLICKHOUSE_CLIENT --query "
    SELECT lifecycle || '(' || lifecycle_reason || ')' FROM system.cas_mounts
    WHERE disk = '${DISK_NAME}'")
[ "${LIFECYCLE}" = "vanished(forgotten)" ] || {
    echo "unexpected lifecycle after FORGET: ${LIFECYCLE}"; exit 1; }

rm -rf "${POOL_DIR:?}"   # safe: FORGET stopped and joined every CAS thread for this disk
