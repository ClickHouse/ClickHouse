#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

# A part that shares no column with the current table structure must still be readable: every
# metadata column is missing from it, so it contributes rows of default values.
#
# `injectRequiredColumns` injects the smallest physical column of the part when none of the requested
# columns is physically present, only to learn the number of rows. It intersects the part's columns
# with the metadata, and when that intersection was empty it fell back to the raw part columns and
# injected one of those, which the caller cannot resolve: `NO_SUCH_COLUMN_IN_TABLE`, naming a column
# that is not in the table. With adaptive granularity the row count comes from the marks, so nothing
# needs to be injected at all.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint is server-global: release it even if a query below fails, or every later test on this
# server would run with mutations disabled.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_all_dropped" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_stale_attach" 2>/dev/null
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

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

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

echo 'renamed columns, non-adaptive granularity'

# With adaptive granularity the row count comes from the marks, so a part with no usable column can
# still be read and the mapping of renamed columns is not observable. With `index_granularity_bytes =
# 0` the granularity is not adaptive and a column really must be read, so this is where the mapping of
# a renamed column to its name in the part carries its weight: without it no candidate is found and
# the read fails.
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

# A single ALTER can rename a column and then drop it under its new name, so a pending rename and a
# pending drop of the same column do coexist. (Two statements cannot: an ALTER issued while a RENAME is
# still pending blocks, even for an unrelated column.) The drop is recorded under the new name while
# the part holds the old one, so both names have to be consulted before concluding that the part holds
# unexplained data.
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

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"
