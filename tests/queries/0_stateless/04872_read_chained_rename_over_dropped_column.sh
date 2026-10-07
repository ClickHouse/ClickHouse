#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# Tag zookeeper: needs ReplicatedMergeTree to keep several ALTERs pending at once
# Tag no-shared-merge-tree: `--replace-replicated-with-shared` rewrites the engine below, and
# SharedMergeTree does not leave the re-attached part with the pending renames this case reads through

# `RENAME a TO b, DROP b, RENAME c TO b` would leave two mappings onto `b` — from the dropped `a` and
# from the live `c` — and a lookup by `b` returns whichever comes first. The mapping of the dropped
# column is obsolete, so `b` has to read `c`.
#
# Here `c` reaches `b` through `d`, so the last rename is applied to the existing `d -> c` entry
# instead of appending a new one. Retiring the obsolete mapping only on the appending path would miss
# it, leaving `b -> a` and `b -> c` side by side again.
#
# A single ALTER rejects transitive renames, and on MergeTree a metadata ALTER always waits for the
# previous mutation, so two of them can never both be pending. The ALTERs run with the partition
# detached, then the part carrying the old column names is attached to a table whose merges are
# stopped, so the mutations stay unapplied while it is read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_chained_reuse_dropped_target" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_chained_reuse_dropped_target SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_chained_reuse_dropped_target SYNC;

CREATE TABLE t_chained_reuse_dropped_target (a UInt64, c UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_chained_reuse_dropped_target', 'r1')
ORDER BY tuple() PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_chained_reuse_dropped_target SELECT number + 100, number + 5000 FROM numbers(1000);

ALTER TABLE t_chained_reuse_dropped_target DETACH PARTITION tuple();

SET alter_sync = 1;
SET mutations_sync = 0;
ALTER TABLE t_chained_reuse_dropped_target RENAME COLUMN a TO b;
ALTER TABLE t_chained_reuse_dropped_target DROP COLUMN b;
ALTER TABLE t_chained_reuse_dropped_target RENAME COLUMN c TO d;
ALTER TABLE t_chained_reuse_dropped_target RENAME COLUMN d TO b;

SYSTEM STOP MERGES t_chained_reuse_dropped_target;
ALTER TABLE t_chained_reuse_dropped_target ATTACH PARTITION tuple();
SYSTEM SYNC REPLICA t_chained_reuse_dropped_target;

SELECT count(), min(b), max(b) FROM t_chained_reuse_dropped_target;

SYSTEM START MERGES t_chained_reuse_dropped_target;
"
