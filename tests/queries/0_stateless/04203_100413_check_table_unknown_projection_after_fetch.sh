#!/usr/bin/env bash
# Tags: replica, no-shared-merge-tree
# Regression test for issue #100413.
#
# Bug: a replica that fetched a part whose `checksums.txt` references a
# projection unknown to the current table schema (e.g. `pp` was dropped
# while the part was detached, then re-attached) entered an infinite
# re-fetch loop on the next `CHECK TABLE` or background
# `ReplicatedMergeTreePartCheckThread` run.  The sender's
# `send_projections` loop skipped the unknown `pp.proj/` subdirectory so
# it was never transferred, but `checksums.txt` was copied verbatim and
# still referenced the orphan entry.  `checkDataPart` then threw
# `NO_FILE_IN_DATA_PART`, the part was marked broken and re-fetched from
# the same replica, producing the same state -- forever.
#
# Fix: strip orphan `.proj` entries from `checksums_txt` before
# `checkEqual`, mirroring the existing handling of unknown projections
# whose directories are present on disk (PR #99623).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run() { ${CLICKHOUSE_CLIENT} --query "$@"; }

REPLICAS=2
for i in $(seq $REPLICAS); do
    run "DROP TABLE IF EXISTS t_unknown_proj_fetch_$i SYNC"
    run "CREATE TABLE t_unknown_proj_fetch_$i (x Int32, y Int32, PROJECTION p (SELECT x, y ORDER BY x))
         ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_unknown_proj_fetch', '$i')
         PARTITION BY intDiv(y, 100) ORDER BY y
         SETTINGS max_parts_to_merge_at_once = 1"
done

run "INSERT INTO t_unknown_proj_fetch_1 SELECT number, number FROM numbers(7)"

# Add and materialize projection `pp` -- creates `pp.proj/` on disk in
# every active part and registers `pp.proj` in their `checksums.txt`.
run "ALTER TABLE t_unknown_proj_fetch_1 ADD PROJECTION pp (SELECT x, count() GROUP BY x) SETTINGS mutations_sync=2, alter_sync=2"
run "ALTER TABLE t_unknown_proj_fetch_1 MATERIALIZE PROJECTION pp SETTINGS mutations_sync=2, alter_sync=2"

# Detach the partition so the parts (with `pp.proj/` and `pp.proj` in
# their on-disk `checksums.txt`) move into `detached/`.
run "ALTER TABLE t_unknown_proj_fetch_1 DETACH PARTITION 0 SETTINGS mutations_sync=2, alter_sync=2"

# Drop projection `pp` while the partition is detached.  After this,
# the table schema no longer knows about `pp` -- but the detached part
# still has `pp.proj/` on disk and `pp.proj` in its `checksums.txt`.
run "ALTER TABLE t_unknown_proj_fetch_1 CLEAR PROJECTION pp SETTINGS mutations_sync=2, alter_sync=2"
run "ALTER TABLE t_unknown_proj_fetch_1 DROP PROJECTION pp SETTINGS mutations_sync=2, alter_sync=2"

# Drop replica 2's detached copy of the partition so that when we
# attach on replica 1 and `SYSTEM SYNC REPLICA t_unknown_proj_fetch_2`,
# replica 2 must FETCH the part rather than re-attach its own detached
# copy.  This is the path that exercises the bug.
run "ALTER TABLE t_unknown_proj_fetch_2 DROP DETACHED PARTITION 0 SETTINGS allow_drop_detached=1"

# Re-attach on replica 1.  The active part still carries the orphan
# `pp.proj/` directory on disk and the `pp.proj` entry in checksums.
run "ALTER TABLE t_unknown_proj_fetch_1 ATTACH PARTITION 0 SETTINGS mutations_sync=2"

# Force replica 2 to fetch the part from replica 1.  The sender's
# `getProjectionParts` returns nothing for the unknown projection, so
# the `pp.proj/` subdirectory is not transferred -- but `checksums.txt`
# is copied verbatim and still references `pp.proj`.
run "SYSTEM SYNC REPLICA t_unknown_proj_fetch_2"

# Sanity: data is intact on both replicas.
run "SELECT count() FROM t_unknown_proj_fetch_1"
run "SELECT count() FROM t_unknown_proj_fetch_2"

# The fix: `CHECK TABLE` on the fetching replica must succeed.
# Pre-fix, this threw `NO_FILE_IN_DATA_PART` for `pp.proj` and triggered
# the re-fetch loop.
run "CHECK TABLE t_unknown_proj_fetch_2 SETTINGS check_query_single_value_result = 1"

# A merge over the fetched part must also succeed (exercises the
# read path under a different caller).
run "ALTER TABLE t_unknown_proj_fetch_1 MODIFY SETTING max_parts_to_merge_at_once = 100"
run "OPTIMIZE TABLE t_unknown_proj_fetch_1 FINAL"
run "SYSTEM SYNC REPLICA t_unknown_proj_fetch_2"
run "SELECT sum(x), sum(y) FROM t_unknown_proj_fetch_2"

for i in $(seq $REPLICAS); do
    run "DROP TABLE IF EXISTS t_unknown_proj_fetch_$i SYNC"
done
