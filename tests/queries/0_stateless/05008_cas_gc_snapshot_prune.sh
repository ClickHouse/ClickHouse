#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# End-to-end: a content-addressed (CA) GC round physically deletes unreferenced objects ONLY through
# the ack-floor retired-cursor pipeline — a blob is condemned (stage 1), floor-passed / republished as
# `delete_pending` (stage 2), then exact-token deleted (stage 3), surfaced as `entries_redeleted` /
# `objects_deleted` in `system.cas_gc_log`. We build a named inline CA
# disk, create garbage (INSERT then TRUNCATE), then run synchronous GC rounds in a retry loop until a
# round reports a physical delete — graduation (and so the physical delete) is round-paced, gated on
# `condemn_round < current_round` in `settleEntry` (`renewWatermarkOnce` exists but does not gate it),
# so we poll for a round boundary to pass rather than assume a fixed round count. Once a delete is
# observed we assert every physical delete went through stage 3 (`entries_redeleted >= objects_deleted`):
# the redelete loop is the SOLE content-delete site and counts one redelete per attempt
# (Deleted/Absent/Replaced), so this inequality is a structural identity of the pipeline and an ad-hoc
# delete that bypassed the graduate->redelete path would break it. This proves deletion fires
# end-to-end through the real SystemLog path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK="05008_cas_gc_snapshot_prune"

# CA-over-LOCAL object storage emits a one-time <Warning> about emulated conditional operations on
# mount; the .sh harness fails on ANY client stderr, so send only error+ logs to the client (real
# errors still surface and fail the test; the expected mount warning does not).
CLIENT="$CLICKHOUSE_CLIENT --send_logs_level=error"

$CLIENT -q "DROP TABLE IF EXISTS t_ca_p9"

$CLIENT -q "
CREATE TABLE t_ca_p9 (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${DISK}',
    name = '${DISK}',
    path = '${DISK}_pool/',
    cas_gc_enabled = 1,
    cas_gc_interval_sec = 1),
    old_parts_lifetime = 1"

# Two distinct inserts => distinct blobs (not deduplicated away); TRUNCATE drops every ref so the
# blobs/trees become unreferenced GC fodder.
$CLIENT -q "INSERT INTO t_ca_p9 SELECT number, toString(number) FROM numbers(1000)"
$CLIENT -q "INSERT INTO t_ca_p9 SELECT number, toString(number) FROM numbers(1000, 1000)"
$CLIENT -q "TRUNCATE TABLE t_ca_p9"

# Run synchronous rounds until a physical delete is observed (bounded retries; graduation is
# round-paced, so we poll for a round boundary to pass rather than assume a fixed round count).
deleted=0
for _ in $(seq 1 40); do
    $CLIENT -q "SYSTEM CAS GC RUN '${DISK}'" > /dev/null
    $CLIENT -q "SYSTEM FLUSH LOGS cas_gc_log"
    deleted=$($CLIENT -q "
        SELECT sum(objects_deleted)
        FROM system.cas_gc_log
        WHERE disk_name LIKE '%${DISK}%' AND event_type = 'Finish'")
    if [ "${deleted:-0}" -gt 0 ]; then break; fi
    sleep 0.5
done

# A physical delete happened, and every physical delete went through stage 3 of the retired-cursor
# pipeline (`entries_redeleted >= objects_deleted`, a structural identity: the redelete loop is the sole
# content-delete site and increments `redeleted` once per attempt). Expect: "deleted>0 redeleted>=deleted"
# => 1 1.
$CLIENT -q "
SELECT
    sum(objects_deleted) > 0,
    sum(entries_redeleted) >= sum(objects_deleted)
FROM system.cas_gc_log
WHERE disk_name LIKE '%${DISK}%' AND event_type = 'Finish'"

$CLIENT -q "DROP TABLE t_ca_p9"
