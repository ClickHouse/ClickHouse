#!/usr/bin/env bash
# Tags: no-parallel, zookeeper
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads
# Tag zookeeper: the last case needs ReplicatedMergeTree to keep several ALTERs pending at once

# A column that was renamed, dropped and then added again in one ALTER must read as its new default,
# not as the data of the column that was dropped.
#
# `columns_to_read` in the reader holds the name each column has in the part, while a drop is recorded
# under the name the ALTER command used. `RENAME a TO b, DROP b, ADD b` leaves the part holding `a`
# with a drop recorded for `b`, so checking only the part-side name misses the drop and the new `b`
# silently returns the old `a` values. Without a rename the two names coincide, which is why the plain
# drop-and-re-add case has always worked.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint is server-global: release it even if a query below fails, or every later test on this
# server would run with mutations disabled.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_readd_wide" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_readd_compact" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_readd_no_rename" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_rename_only" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_drop_then_reuse_name" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_reuse_dropped_target" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_chained_reuse_dropped_target" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_chained_reuse_dropped_target SYNC" 2>/dev/null
}
trap cleanup EXIT

for part_type in wide compact; do
    if [ "$part_type" = "wide" ]; then
        table=t_readd_wide
        part_settings="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
    else
        table=t_readd_compact
        part_settings="min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000"
    fi

    echo "renamed, dropped and re-added ($part_type)"

    $CLICKHOUSE_CLIENT -mq "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (a UInt64, h UInt8 DEFAULT 0)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS $part_settings;

    INSERT INTO $table SELECT number + 100, 0 FROM numbers(1000);

    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

    SET alter_sync = 0;
    ALTER TABLE $table RENAME COLUMN a TO b, DROP COLUMN b, ADD COLUMN b UInt64 DEFAULT 7;

    SELECT count(), min(b), max(b) FROM $table;
    SELECT part_type FROM system.parts WHERE table = '$table' AND database = currentDatabase() AND active;

    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    "
done

echo 'dropped and re-added without a rename'

# The name in the part and the name the drop was recorded under coincide here, so this already worked;
# it guards the behaviour the check was originally written for.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_readd_no_rename;

CREATE TABLE t_readd_no_rename (b UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_readd_no_rename SELECT number + 100, 0 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_readd_no_rename DROP COLUMN b, ADD COLUMN b UInt64 DEFAULT 7;

SELECT count(), min(b), max(b) FROM t_readd_no_rename;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'renamed only'

# A pending rename with no drop must still read the renamed column's data through its old file.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_rename_only;

CREATE TABLE t_rename_only (a UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_rename_only SELECT number + 100, 0 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_rename_only RENAME COLUMN a TO b;

SELECT count(), min(b), max(b) FROM t_rename_only;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'dropped, then the name reused by a rename'

# The mirror of the first case, and it must NOT read as a default. Here the drop comes first, so it hit
# a different column that merely had that name, and the renamed column's data is still live. The two
# cases leave `AlterConversions` holding the same rename and the same dropped name, so only the position
# of the drop among the renames tells them apart.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_drop_then_reuse_name;

CREATE TABLE t_drop_then_reuse_name (key UInt64, value2_old UInt64, value2 UInt64)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_drop_then_reuse_name SELECT number, 1, number + 100 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_drop_then_reuse_name DROP COLUMN value2_old, RENAME COLUMN value2 TO value2_old;

SELECT count(), min(value2_old), max(value2_old) FROM t_drop_then_reuse_name;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'the dropped name taken over by another rename'

# `RENAME a TO b, DROP b, RENAME c TO b` would leave two mappings onto `b` — from the dropped `a` and
# from the live `c` — and a lookup by `b` returns whichever comes first. The mapping of the dropped
# column is obsolete, so `b` has to read `c`.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_reuse_dropped_target;

CREATE TABLE t_reuse_dropped_target (a UInt64, c UInt64, k UInt8 DEFAULT 0)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_reuse_dropped_target SELECT number + 100, number + 5000, 0 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_reuse_dropped_target RENAME COLUMN a TO b, DROP COLUMN b, RENAME COLUMN c TO b;

SELECT count(), min(b), max(b) FROM t_reuse_dropped_target;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'the dropped name taken over by a chained rename'

# Same as above, except that `c` reaches `b` through `d`, so the last rename is applied to the existing
# `d -> c` entry instead of appending a new one. Retiring the obsolete mapping only on the appending
# path would miss it, leaving `b -> a` and `b -> c` side by side again.
#
# This needs ReplicatedMergeTree: a single ALTER rejects transitive renames, and on MergeTree a
# metadata ALTER always waits for the previous mutation, so two of them can never both be pending. The
# ALTERs run with the partition detached, then the part carrying the old column names is attached to a
# table whose merges are stopped, so the mutations stay unapplied while it is read.
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
