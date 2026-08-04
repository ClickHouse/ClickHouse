#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# B3: system.cas_mounts exposes per-disk GC health (is_leader / pending_reclaim /
# last_success_age_seconds / wedged_namespace_count), replacing the retired process-global
# CasGcIsLeader / CasGcPendingReclaimEntries CurrentMetrics gauges (clobbered with >= 2 CAS disks).
# Build one named inline CA disk, run a synchronous GC round so this process has led at least once,
# then assert the column shapes on the healthy single-disk fixture.
#
# This is a .sh test (not .sql) because `SYSTEM CAS GC RUN` now returns a
# one-row-per-disk result set (UX pass); the round below only cares about its side effect (leading
# once), so its own output is redirected to /dev/null.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP TABLE IF EXISTS t_cas_mounts_gc_health;

CREATE TABLE t_cas_mounts_gc_health (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05010',
    name = '05010_cas_mounts_gc_health',
    path = '05010_cas_mounts_gc_health_pool/',
    gc_enabled = 1,
    gc_interval_sec = 1),
    old_parts_lifetime = 1;

INSERT INTO t_cas_mounts_gc_health SELECT number, toString(number) FROM numbers(100);
TRUNCATE TABLE t_cas_mounts_gc_health;
"""

${CLICKHOUSE_CLIENT} -q "SYSTEM CAS GC RUN '05010_cas_mounts_gc_health'" > /dev/null

${CLICKHOUSE_CLIENT} --multiline -q """
SELECT is_leader, wedged_namespace_count
FROM system.cas_mounts
WHERE disk LIKE '%05010_cas_mounts_gc_health%';

SELECT pending_reclaim >= 0, last_success_age_seconds < 60
FROM system.cas_mounts
WHERE disk LIKE '%05010_cas_mounts_gc_health%';

DROP TABLE t_cas_mounts_gc_health;
SELECT 'ok';
"""
