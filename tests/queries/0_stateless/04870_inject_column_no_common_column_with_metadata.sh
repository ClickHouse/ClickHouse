#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

# A part that shares no column with the current table structure must still be readable: every
# metadata column is missing from it, so it contributes rows of default values.
#
# `injectRequiredColumns` used to inject the smallest physical column of the part when none of the
# requested columns was physically present, only to learn the number of rows. It intersected the
# part's columns with the metadata, and when that intersection was empty it fell back to the raw part
# columns and injected one of those, which the caller cannot resolve: `NO_SUCH_COLUMN_IN_TABLE`,
# naming a column that is not in the table. Nothing is injected any more -- the row count comes from
# the index granularity, which is exact for non-adaptive parts too -- and what the function does
# instead is to refuse a part holding data that neither the structure nor a pending conversion
# accounts for. The cases below pin both halves: which parts read as rows of defaults, and which
# must not.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint is server-global: release it even if a query below fails, or every later test on this
# server would run with mutations disabled.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_all_dropped" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_all_dropped_non_adaptive" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dropped_readded" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_stale_attach" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_stale_attach_mixed" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_non_adaptive_rename" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_rename_then_drop" 2>/dev/null
}
trap cleanup EXIT

echo 'all columns dropped'

# Every original column is dropped while its mutation is still pending, so the parts on disk hold only
# columns that the metadata no longer knows. Each DROP passes the "empty parts are not allowed" check
# because that is evaluated per command against the metadata of the moment.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_all_dropped;

CREATE TABLE t_all_dropped (a UInt8, b UInt8)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_all_dropped VALUES (1, 10);
INSERT INTO t_all_dropped VALUES (2, 20);

ALTER TABLE t_all_dropped ADD COLUMN c UInt64 DEFAULT 42;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_all_dropped DROP COLUMN a;
ALTER TABLE t_all_dropped DROP COLUMN b;

SELECT c FROM t_all_dropped ORDER BY c;
SELECT count() FROM t_all_dropped;
OPTIMIZE TABLE t_all_dropped FINAL;
SELECT c FROM t_all_dropped ORDER BY c;
"

# Drop the table before releasing the failpoint. The pending mutation would drop every column the
# part holds, and a mutation that leaves a part with no columns aborts the server -- a separate
# pre-existing defect. This test's subject is the read path, so do not let that mutation run.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_all_dropped SYNC"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"

echo 'all columns dropped, non-adaptive granularity'

# The same part with `index_granularity_bytes = 0`, so the granularity is not adaptive. The row count
# still does not need a column: `MergeTreeIndexGranularityConstant::fixFromRowsCount` makes the last
# granule exact for non-adaptive parts too, so this reads as rows of defaults just like the adaptive
# case above.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_all_dropped_non_adaptive;

CREATE TABLE t_all_dropped_non_adaptive (a UInt8, b UInt8)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity_bytes = 0;

INSERT INTO t_all_dropped_non_adaptive VALUES (1, 10);
INSERT INTO t_all_dropped_non_adaptive VALUES (2, 20);

ALTER TABLE t_all_dropped_non_adaptive ADD COLUMN c UInt64 DEFAULT 42;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_all_dropped_non_adaptive DROP COLUMN a;
ALTER TABLE t_all_dropped_non_adaptive DROP COLUMN b;

SELECT c, count() FROM t_all_dropped_non_adaptive GROUP BY c;
"

# Drop the table before releasing the failpoint, so the pending mutations never run.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_all_dropped_non_adaptive SYNC"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"

echo 'dropped and readded columns, non-adaptive granularity'

# Every column the part holds is dropped and immediately added back under the same name, so each one
# resolves against the metadata while its data in the part is stale. Nothing can be injected, and the
# part is not suspicious either -- refusing this read would be a regression: the rows are legitimately
# rows of defaults.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_dropped_readded;

CREATE TABLE t_dropped_readded (a UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity_bytes = 0;

INSERT INTO t_dropped_readded SELECT number, 0 FROM numbers(10000);

ALTER TABLE t_dropped_readded ADD COLUMN value UInt64 DEFAULT 42;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_dropped_readded DROP COLUMN a;
ALTER TABLE t_dropped_readded ADD COLUMN a UInt64 DEFAULT 7;
ALTER TABLE t_dropped_readded DROP COLUMN h;
ALTER TABLE t_dropped_readded ADD COLUMN h UInt8 DEFAULT 9;

SELECT count(), min(value), max(value) FROM t_dropped_readded;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dropped_readded SYNC"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"

echo 'stale part re-attached'

# The opposite case, and it must NOT read as defaults. Here the DROP is already applied, so nothing
# records why the part still holds `a`: it is a part attached after the schema moved on. Its rows do
# exist on disk, so reporting defaults for them would hide data. The read has to fail — same principle
# as 04011_detach_rename_attach_column (issue #79110), which covers the renamed variant.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_stale_attach;

CREATE TABLE t_stale_attach (a UInt64)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_attach SELECT number FROM numbers(1000);

ALTER TABLE t_stale_attach DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_attach ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_stale_attach DROP COLUMN a SETTINGS alter_sync = 2;
ALTER TABLE t_stale_attach ATTACH PART 'all_1_1_0';

SELECT count(), min(b), max(b) FROM t_stale_attach;
" 2>&1 | grep -oF 'NO_SUCH_COLUMN_IN_TABLE' | head -1

echo 'stale part re-attached alongside a column the structure knows'

# The same stale part, except that it also holds `k`, which the structure still knows. Reading a
# column the part does not have therefore has a candidate to serve the row count, and the older
# behaviour was to inject `k` and report `b` as its default -- silently hiding the rows of `a` that
# are on disk. The part is refused for the same reason as above: what decides is whether everything
# the part holds is accounted for, not whether some column of it happens to be readable.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_stale_attach_mixed;

CREATE TABLE t_stale_attach_mixed (a UInt64, k UInt64)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_stale_attach_mixed SELECT number, number FROM numbers(1000);

ALTER TABLE t_stale_attach_mixed DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_attach_mixed ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_stale_attach_mixed DROP COLUMN a SETTINGS alter_sync = 2;
ALTER TABLE t_stale_attach_mixed ATTACH PART 'all_1_1_0';

SELECT min(b), max(b) FROM t_stale_attach_mixed;
" 2>&1 | grep -oF 'NO_SUCH_COLUMN_IN_TABLE' | head -1

echo 'renamed columns'

# What this pins is the mapping of a part's column name through a pending rename. The part holds `a`
# and `h`, the structure knows them as `c` and `d`, and only that mapping tells the two apart from
# unexplained data -- without it the part looks like a stale attach and the read is refused. That
# holds under either granularity; `index_granularity_bytes = 0` here makes the part's marks
# non-adaptive, so the row count of a read that touches no column is exercised as well.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_non_adaptive_rename;

CREATE TABLE t_non_adaptive_rename (a UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity_bytes = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0;

INSERT INTO t_non_adaptive_rename SELECT number, 0 FROM numbers(10000);
INSERT INTO t_non_adaptive_rename SELECT number + 10000, 0 FROM numbers(10000);

ALTER TABLE t_non_adaptive_rename ADD COLUMN value UInt64 DEFAULT 42;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_non_adaptive_rename RENAME COLUMN a TO c, RENAME COLUMN h TO d;

SELECT count(), min(value), max(value) FROM t_non_adaptive_rename;
OPTIMIZE TABLE t_non_adaptive_rename FINAL;
SELECT count(), min(value), max(value) FROM t_non_adaptive_rename;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'pending rename and drop of the same column'

# A single ALTER can rename a column and then drop it under its new name. `addMutationCommand` folds
# that pair into one entry -- the drop is recorded under the name the part holds and the rename mapping
# is erased -- so the part-side name alone answers whether the column is being dropped. This pins that
# folding: were the drop recorded under the new name instead, the part would look like it holds
# unexplained data and the read would be refused.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_rename_then_drop;

CREATE TABLE t_rename_then_drop (a UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_rename_then_drop SELECT number, 0 FROM numbers(1000);

ALTER TABLE t_rename_then_drop ADD COLUMN value UInt64 DEFAULT 42;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_rename_then_drop
    RENAME COLUMN a TO b, RENAME COLUMN h TO d, DROP COLUMN b, DROP COLUMN d;

SELECT count(), min(value), max(value) FROM t_rename_then_drop;
"

# Drop before releasing the failpoint, as above: this ALTER too drops every column of the part.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_rename_then_drop SYNC"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
