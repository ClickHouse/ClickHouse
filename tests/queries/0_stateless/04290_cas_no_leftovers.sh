#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# ^ cas is an object-storage metadata type (keep it off the minimal fasttest image);
#   no-parallel because we inspect a known on-disk pool directory from the shell and must not race
#   another test sharing the same path.

# North-star "no S3 leftovers" oracle for the content-addressed pool, exercised over a `local`
# object_storage backend so the pool is a plain directory the test shell can inspect directly.
#
# We put the pool under CLICKHOUSE_USER_FILES_UNIQUE (an absolute path both the server and this
# shell can see on a local run) and enable the background reachability GC aggressively
# (gc_enabled=1, grace=2s, interval=1s). We then:
#   (1) record the baseline blobs+parts object count (~0),
#   (2) CREATE a MergeTree on the CA disk and INSERT several distinct batches to make many blobs,
#   (3) assert the count rose above baseline,
#   (4) DROP TABLE ... SYNC so the refs are unlinked and the blobs/footers become GC fodder,
#   (5) drain the retire pipeline deterministically via `SYSTEM CAS GC RUN` (bounded
#       loop on the `pending_*` gauges, NOT a fixed sleep), then run `FSCK` directly on the running
#       disk (T13): a clean reachability audit reading back zero `unreachable`/`dangling` is a
#       strictly stronger no-leftovers oracle than polling the pool directory ever was.
# `_pool_meta` (durable single-owner marker) and the `store/` metadata tree are expected to remain.
# Teardown is fail-closed (spec rev.8 §5/§9): `SYSTEM CAS FORGET` the disk (force-Vanish,
# node-local), verify it reads `vanished(forgotten)` in system.cas_mounts, and only then
# `rm -rf` — FORGET stopped and joined every CAS background thread for this disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POOL_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}_04290_${RANDOM}"

# Fresh pool dir for this run.
rm -rf "${POOL_DIR:?}"
mkdir -p "${POOL_DIR}"

# Count regular files (objects) currently living under blobs/ and parts/ in the pool.
count_pool_objects() {
    local n_blobs n_parts
    n_blobs=$(find "${POOL_DIR}/ca/blobs" "${POOL_DIR}/ca/packs" -type f 2>/dev/null | wc -l)
    n_parts=$(find "${POOL_DIR}/ca/trees" -type f 2>/dev/null | wc -l)
    echo $(( n_blobs + n_parts ))
}

DISK_NAME="ca_04290_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
DISK_DEF="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04290',
    name = '${DISK_NAME}',
    path = '${POOL_DIR}/',
    gc_enabled = 1,
    gc_interval_sec = 1)"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_cas_leftovers SYNC"

# (1) Baseline.
BASELINE=$(count_pool_objects)

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_cas_leftovers (a UInt64, s String, d Date)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = ${DISK_DEF}"

# (2) Several distinct inserts -> several distinct parts/blobs (distinct data => no dedup-away).
for i in 0 1 2 3 4 5; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO t_cas_leftovers
        SELECT number + ${i} * 100000, toString(number + ${i} * 100000), toDate('2020-01-01') + (number % 1000)
        FROM numbers(100000)"
done

$CLICKHOUSE_CLIENT --query "SELECT 'rows', count() FROM t_cas_leftovers"

# (3) Pool must have grown above baseline.
AFTER_INSERT=$(count_pool_objects)
if [ "$AFTER_INSERT" -gt "$BASELINE" ]; then
    echo "grew_above_baseline 1"
else
    echo "grew_above_baseline 0 (baseline=${BASELINE} after_insert=${AFTER_INSERT})"
fi

# (4) Drop: refs unlinked synchronously, blobs/footers become unreferenced GC fodder.
$CLICKHOUSE_CLIENT --query "DROP TABLE t_cas_leftovers SYNC"

# (5) Drain GC deterministically: loop `SYSTEM CAS GC RUN` rounds until the retire
#     pipeline's `pending_*` gauges (Task 7) read back to empty. Bounded (~60 rounds, half-second
#     spacing), not a fixed sleep; column values are looked up BY HEADER NAME (not position) so the
#     loop keeps working if the result set gains columns.
PENDING=1
for _ in $(seq 1 60); do
    PENDING=$($CLICKHOUSE_CLIENT --query "SYSTEM CAS GC RUN '${DISK_NAME}'" --format TSVWithNames \
        | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                      { print $col["pending_condemned"] }   # already candidates+retired per its doc in Gc/CasGc.h; summing all three double-counts')
    [ "${PENDING}" = "0" ] && break
    sleep 0.5
done

if [ "${PENDING}" != "0" ]; then
    echo "FAIL: GC did not drain the retire pipeline within the bounded loop (pending=${PENDING})" >&2
    exit 1
fi

# (6) FSCK runs directly on the running disk (T13): a reachability audit that must read back zero
#     unreachable/dangling objects. This is a strictly stronger no-leftovers oracle than the old
#     dir-poll.
$CLICKHOUSE_CLIENT --query "SYSTEM CAS FSCK '${DISK_NAME}'" --format TSVWithNames \
    | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                  { print "fsck_unreachable", $col["unreachable"]; print "fsck_dangling", $col["dangling"] }'

# _pool_meta must still be present (durable single-owner marker is never GC'd).
if [ -f "${POOL_DIR}/ca/_pool_meta" ]; then
    echo "pool_meta_present 1"
else
    echo "pool_meta_present 0"
fi

# (7) Fail-closed teardown (spec rev.8 §5/§9): FORGET the disk (force-Vanish, node-local; the table is
#     already dropped above), verify it reads exactly `vanished(forgotten)` in the mounts table, and
#     only then rm. A failed FORGET or an unexpected lifecycle aborts with the pool dir left in place.
#     FORGET logs an operator WARNING; the harness runs the client at --send_logs_level=warning, so that
#     expected warning would stream to stderr and be flagged as a failure -- suppress it for this call.
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${DISK_NAME}'" || {
    echo "FORGET failed — leaving pool dir in place (fail-closed)"; exit 1; }
LIFECYCLE=$($CLICKHOUSE_CLIENT --query "
    SELECT lifecycle || '(' || lifecycle_reason || ')' FROM system.cas_mounts
    WHERE disk = '${DISK_NAME}'")
[ "${LIFECYCLE}" = "vanished(forgotten)" ] || {
    echo "unexpected lifecycle after FORGET: ${LIFECYCLE}"; exit 1; }

rm -rf "${POOL_DIR:?}"   # safe: FORGET stopped and joined every CAS thread for this disk
