#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

# A part produced by a merge while an `ALTER TABLE ... RENAME COLUMN` is still pending already stores
# the new column name, because the merge writes the current structure. The rename nevertheless still
# looks pending for that part: what decides that on plain MergeTree is the part's data version, and a
# merge keeps the data version of its sources. Applying the rename a second time on read looks for a
# file under the old name, does not find it, and returns the column's default -- silently wrong values
# for data that is intact on disk. The mapping must therefore only be applied to a part that actually
# holds the old name, which is how `MutateTask` already guards its own renames.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The failpoint is server-global: release it even if a query below fails, or every later test on this
# server would run with mutations disabled.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_merge_pending_rename" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_merge_pending_rename_all" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_pending_rename_name_reuse" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_merge_pending_rename_compact" 2>/dev/null
}
trap cleanup EXIT

echo 'one column renamed'

$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_merge_pending_rename;

CREATE TABLE t_merge_pending_rename (k UInt64, a UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_merge_pending_rename SELECT number, number + 100 FROM numbers(1000);
INSERT INTO t_merge_pending_rename SELECT number + 1000, number + 200 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_merge_pending_rename RENAME COLUMN a TO c;

-- Reading the source parts, which still hold the old name.
SELECT count(), min(c), max(c) FROM t_merge_pending_rename;

OPTIMIZE TABLE t_merge_pending_rename FINAL;

-- Reading the merged part, which already holds the new name while the rename is still pending.
SELECT count(), min(c), max(c) FROM t_merge_pending_rename;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'every column renamed'

# With no column left under its old name the merged part shares no name with the source parts, which is
# also the shape in which a merge reads a part that can serve none of the columns it asks for.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_merge_pending_rename_all;

CREATE TABLE t_merge_pending_rename_all (a UInt64, h UInt8 DEFAULT 0)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0;

INSERT INTO t_merge_pending_rename_all SELECT number, 1 FROM numbers(1000);
INSERT INTO t_merge_pending_rename_all SELECT number + 1000, 1 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_merge_pending_rename_all RENAME COLUMN a TO c, RENAME COLUMN h TO d;

OPTIMIZE TABLE t_merge_pending_rename_all FINAL;

SELECT count(), min(c), max(c), min(d), max(d) FROM t_merge_pending_rename_all;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'one column renamed, compact part'

# The same defect on compact parts: the column list lives in one file rather than per-column files, but
# the merged part is written under the current structure just the same.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_merge_pending_rename_compact;

CREATE TABLE t_merge_pending_rename_compact (k UInt64, a UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_merge_pending_rename_compact SELECT number, number + 100 FROM numbers(1000);
INSERT INTO t_merge_pending_rename_compact SELECT number + 1000, number + 200 FROM numbers(1000);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_merge_pending_rename_compact RENAME COLUMN a TO c;

OPTIMIZE TABLE t_merge_pending_rename_compact FINAL;

SELECT count(), min(c), max(c) FROM t_merge_pending_rename_compact;
SELECT part_type FROM system.parts WHERE table = 't_merge_pending_rename_compact' AND database = currentDatabase() AND active;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"

echo 'a freed name handed to another column'

# The name a rename frees can be given to a different column in the same statement. A source part then
# holds `A` containing what is now `C`, so a request for the new `A` must NOT settle for that file just
# because the part happens to have a column of that name -- it has to map to `B`, find it absent, and
# read the default. This is the case that a plain "does the part hold the old name" test gets wrong.
#
# Only the source part is read here on purpose. Running a merge in this state is a separate, still
# unfixed defect: the merged part then holds `C` and `A`, both current names, and nothing distinguishes
# them from a pre-rename part, so `C` reads what is now `A`. That is pre-existing -- master produces the
# identical wrong output -- and cannot be decided in the read path, so it is tracked separately rather
# than pinned here.
$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t_pending_rename_name_reuse;

CREATE TABLE t_pending_rename_name_reuse (k UInt64, A UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_pending_rename_name_reuse SELECT number, number + 100 FROM numbers(3);

ALTER TABLE t_pending_rename_name_reuse ADD COLUMN B UInt64 DEFAULT 7;

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_pending_rename_name_reuse RENAME COLUMN A TO C, RENAME COLUMN B TO A;

SELECT A, C FROM t_pending_rename_name_reuse ORDER BY k;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"
