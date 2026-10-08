#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ALTER MODIFY COLUMN from Nullable to non-Nullable replaces NULLs with the DEFAULT, which reads other columns of the row.

$CLICKHOUSE_CLIENT <<'EOF'
SELECT 'compact and wide parts, sync';
CREATE TABLE t_compact (c0 Int64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000;
CREATE TABLE t_wide (c0 Int64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_compact VALUES (1, NULL), (2, 5);
INSERT INTO t_wide VALUES (1, NULL), (2, 5);
SELECT table, part_type FROM system.parts WHERE database = currentDatabase() AND table IN ('t_compact', 't_wide') AND active ORDER BY table;
ALTER TABLE t_compact MODIFY COLUMN a Int64 DEFAULT c0 + 100 SETTINGS mutations_sync = 2;
ALTER TABLE t_wide MODIFY COLUMN a Int64 DEFAULT c0 + 100 SETTINGS mutations_sync = 2;
SELECT c0, a FROM t_compact ORDER BY c0;
SELECT c0, a FROM t_wide ORDER BY c0;
SELECT table, is_done FROM system.mutations WHERE database = currentDatabase() AND table IN ('t_compact', 't_wide') ORDER BY table;
SELECT table, type FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t_compact', 't_wide') AND column = 'a' AND active ORDER BY table;

SELECT 'reads before the mutation';
CREATE TABLE t_fly (c0 Int64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fly VALUES (1, NULL), (2, 5), (3, 1000);
SYSTEM STOP MERGES t_fly;
ALTER TABLE t_fly MODIFY COLUMN a Int64 DEFAULT c0 + 100 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT a FROM t_fly ORDER BY a;
SELECT a FROM t_fly PREWHERE c0 > 0 ORDER BY a SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT c0 FROM t_fly PREWHERE a > 100 ORDER BY c0 SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (EXPLAIN SELECT a FROM t_fly ORDER BY c0 LIMIT 1)
    SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT a FROM t_fly ORDER BY c0 LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SYSTEM START MERGES t_fly;
OPTIMIZE TABLE t_fly FINAL;
SELECT c0, a FROM t_fly ORDER BY c0;

SELECT 'existing DEFAULT kept by MODIFY';
CREATE TABLE t_keep (c0 Int64, a Nullable(Int64) DEFAULT c0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_keep VALUES (1, NULL), (2, 5);
ALTER TABLE t_keep MODIFY COLUMN a Int64 SETTINGS mutations_sync = 2;
SELECT c0, a FROM t_keep ORDER BY c0;

SELECT 'DEFAULT changed while the conversion is pending';
CREATE TABLE t_meta (c0 Int64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_meta VALUES (1, NULL), (2, 5);
SYSTEM STOP MERGES t_meta;
ALTER TABLE t_meta MODIFY COLUMN a Int64 DEFAULT 7 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_meta MODIFY COLUMN a DEFAULT c0 + 100 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT a FROM t_meta ORDER BY a;
SYSTEM START MERGES t_meta;
OPTIMIZE TABLE t_meta FINAL;
SELECT c0, a FROM t_meta ORDER BY c0;

SELECT 'DEFAULT reads a MATERIALIZED column';
CREATE TABLE t_mat (c0 Int64, m Int64 MATERIALIZED c0 * 3, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mat (c0, a) VALUES (1, NULL), (2, 5);
ALTER TABLE t_mat MODIFY COLUMN a Int64 DEFAULT m SETTINGS mutations_sync = 2;
SELECT c0, a FROM t_mat ORDER BY c0;

SELECT 'DEFAULT reads a column converted too';
CREATE TABLE t_conv (c0 Float64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_conv VALUES (1.7, NULL), (2.2, 5);
SYSTEM STOP MERGES t_conv;
ALTER TABLE t_conv MODIFY COLUMN c0 Int64 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_conv MODIFY COLUMN a Int64 DEFAULT c0 * 10 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT a FROM t_conv ORDER BY a;
SELECT c0, a FROM t_conv ORDER BY c0;
SYSTEM START MERGES t_conv;
OPTIMIZE TABLE t_conv FINAL;
SELECT c0, a FROM t_conv ORDER BY c0;

SELECT 'NULL written by a patch part before the ALTER';
CREATE TABLE t_patch (c0 Int64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch VALUES (1, 4), (2, 5);
UPDATE t_patch SET a = NULL WHERE c0 = 2;
SYSTEM STOP MERGES t_patch;
ALTER TABLE t_patch MODIFY COLUMN a Int64 DEFAULT c0 + 100 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT a FROM t_patch ORDER BY a;
SYSTEM START MERGES t_patch;
OPTIMIZE TABLE t_patch FINAL;
SELECT c0, a FROM t_patch ORDER BY c0;

SELECT 'DEFAULT dependency updated by a patch part after the ALTER';
CREATE TABLE t_patch_after (c0 Int64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch_after VALUES (1, NULL), (2, 5);
SYSTEM STOP MERGES t_patch_after;
ALTER TABLE t_patch_after MODIFY COLUMN a Int64 DEFAULT c0 + 100 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_patch_after SET c0 = c0 + 10 WHERE 1;
SELECT c0, a FROM t_patch_after ORDER BY c0;
EOF

# A dependency read by an earlier step may hold a value written after the ALTER.
$CLICKHOUSE_CLIENT -q "SELECT a FROM t_patch_after PREWHERE c0 > 0 ORDER BY a SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'

$CLICKHOUSE_CLIENT <<'EOF'
SELECT 'LowCardinality(Nullable(String)) to String';
CREATE TABLE t_lc (c0 Int64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc VALUES (1, NULL), (2, 'x');
ALTER TABLE t_lc MODIFY COLUMN s String DEFAULT toString(c0) SETTINGS mutations_sync = 2;
SELECT c0, s FROM t_lc ORDER BY c0;
EOF

# The conversion of `a` must read the converted `c0`, whatever the server's `prefer_column_name_to_alias` is.
echo 'DEFAULT reads a column converted too, prefer_column_name_to_alias = 1'
$CLICKHOUSE_LOCAL --prefer_column_name_to_alias=1 --multiquery "
CREATE TABLE t_conv (c0 Float64, a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_conv VALUES (1.7, NULL), (2.2, 5);
SYSTEM STOP MERGES t_conv;
ALTER TABLE t_conv MODIFY COLUMN c0 Int64 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_conv MODIFY COLUMN a Int64 DEFAULT c0 * 10 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT c0, a FROM t_conv ORDER BY c0;
"
