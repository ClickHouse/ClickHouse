#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# Introspection coverage for the content-addressed (CA) garbage collector: the
# `SYSTEM CAS GC RUN <disk>` command runs one GC round synchronously and
# the round is recorded in `system.cas_gc_log` (a Start + Finish row
# per round, like `part_log`). We build a CA disk inline (named, so the SYSTEM command can target it),
# create garbage by inserting then truncating, run the round a few times, flush the log, and assert
# the rows are there with the right shape — including a non-empty per-round `ProfileEvents` delta
# (the Manual round runs on the query thread, which always has an attached ThreadStatus that captures
# ProfileEvents).
#
# This is a .sh test (not .sql) because `SYSTEM CAS GC RUN` now returns a
# one-row-per-disk result set (UX pass); the three synchronous rounds below only care about their
# side effects on the log, so their own output is redirected to /dev/null.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP TABLE IF EXISTS t_cas_gc_introspection;

-- A named inline CA disk: the \`name\` is what \`SYSTEM CAS GC RUN <name>\`
-- targets and what lands in the log's \`disk_name\` column.
CREATE TABLE t_cas_gc_introspection (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05007',
    name = '05007_cas_gc_introspection',
    path = '05007_cas_gc_introspection_pool/',
    gc_enabled = 1,
    gc_interval_sec = 1),
    old_parts_lifetime = 1;

-- Two distinct inserts => distinct blobs (not deduped away), then TRUNCATE drops every ref so the
-- blobs/trees become unreferenced GC fodder.
INSERT INTO t_cas_gc_introspection SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_cas_gc_introspection SELECT number, toString(number) FROM numbers(1000, 1000);
TRUNCATE TABLE t_cas_gc_introspection;
"""

# Run several synchronous rounds: the first rounds mark the retired candidates, later rounds delete
# them once the durable watermark floor advances past the builds (the background renewer does this).
${CLICKHOUSE_CLIENT} -q "SYSTEM CAS GC RUN '05007_cas_gc_introspection'" > /dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM CAS GC RUN '05007_cas_gc_introspection'" > /dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM CAS GC RUN '05007_cas_gc_introspection'" > /dev/null

${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM FLUSH LOGS cas_gc_log;

-- A Start, a Finish, and a Manual-triggered row were all recorded for this disk.
SELECT
    countIf(event_type = 'Start') > 0,
    countIf(event_type = 'Finish') > 0,
    countIf(trigger = 'Manual') > 0
FROM system.cas_gc_log
WHERE disk_name LIKE '%05007_cas_gc_introspection%';

-- A synchronous Manual Finish captured a non-empty per-round ProfileEvents delta (the round touches
-- the object storage, so Cas*/Disk*/S3* counters are non-zero). The query thread is always attached,
-- so capture is active for the Manual path.
SELECT any(length(ProfileEvents)) > 0
FROM system.cas_gc_log
WHERE disk_name LIKE '%05007_cas_gc_introspection%'
  AND event_type = 'Finish'
  AND trigger = 'Manual';

-- Per-phase rows: a folding round emits one Phase row per phase it reached (19 of them), so a run
-- that folded at least once must show most of the phase vocabulary, including the fold's own
-- ref-prefix enumeration and the round-commit CAS.
SELECT countDistinct(phase) >= 10,
       countIf(phase = 'fold_ref_group') > 0,
       countIf(phase = 'round_commit') > 0
FROM system.cas_gc_log
WHERE disk_name LIKE '%05007_cas_gc_introspection%'
  AND event_type = 'Phase';

-- The correlator: every row of the most recent round of this disk -- its Start, each Phase, and its
-- Finish -- shares one \`round_id\`, and that round emitted at least one Phase row between them.
SELECT countIf(event_type = 'Start') = 1,
       countIf(event_type = 'Finish') = 1,
       countIf(event_type = 'Phase') > 0
FROM system.cas_gc_log
WHERE round_id = (
    SELECT round_id FROM system.cas_gc_log
    WHERE disk_name LIKE '%05007_cas_gc_introspection%' AND event_type = 'Finish'
    ORDER BY event_time_microseconds DESC LIMIT 1);

-- A Phase row's \`ProfileEvents\` is that phase's own delta, not the whole round's: the phase that
-- enumerates the ref prefix must show fewer events than the round summary it is a part of. That
-- enumeration lives in \`defer_decision\`, which owns the \`cas/refs/\` LIST. It is deliberately NOT
-- \`fold_ref_group\`: that phase is I/O-free by construction -- the keys are already in hand -- so its
-- own delta is empty and this assertion would answer 0 there no matter how healthy the round was.
SELECT max(length(ProfileEvents)) > 0
FROM system.cas_gc_log
WHERE disk_name LIKE '%05007_cas_gc_introspection%'
  AND event_type = 'Phase' AND phase = 'defer_decision';

-- The error path: a non-CA disk (the always-present local \`default\`) is rejected.
SYSTEM CAS GC RUN 'default'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_cas_gc_introspection;
SELECT 'ok';
"""
