-- Tags: no-parallel-replicas
-- no-parallel-replicas: `force_optimize_projection` fails without the parallel replicas local plan.
-- Random settings limits: query_plan_optimize_lazy_materialization=(1, 1); query_plan_max_limit_for_lazy_materialization=(10, None); optimize_use_projections=(1, 1); use_top_k_dynamic_filtering=(0, 0); max_insert_threads=(1, 1); optimize_on_insert=(1, 1)
-- Lazily read columns are correct when `_part_offset` / `_part_starting_offset` of the read are not its own row positions.

DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_proj2;
DROP TABLE IF EXISTS t_col;
DROP TABLE IF EXISTS t_start;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_repl;
DROP TABLE IF EXISTS t_repl_plain;

-- A projection that stores `_part_offset` of the parent part, `_part_offset` both filtered and selected.
CREATE TABLE t_proj (a Int32, b Int32, PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_proj SELECT number, (number * 7919) % 100000 FROM numbers(100000);
OPTIMIZE TABLE t_proj FINAL;
SELECT _part_offset, a, b FROM t_proj WHERE b < 5000 AND _part_offset % 2 = 1 ORDER BY _part_offset LIMIT 3 SETTINGS force_optimize_projection = 1;

-- The same with two parts.
CREATE TABLE t_proj2 (a Int32, b Int32, PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_proj2;
INSERT INTO t_proj2 SELECT number, (number * 7919) % 100000 FROM numbers(50000);
INSERT INTO t_proj2 SELECT number, (number * 7919) % 100000 FROM numbers(50000, 50000);
SELECT _part_offset, a, b FROM t_proj2 WHERE b < 5000 AND _part_offset % 2 = 1 ORDER BY _part_offset, b LIMIT 4 SETTINGS force_optimize_projection = 1;
SELECT _part_offset, a, b FROM t_proj2 WHERE b < 5000 AND _part_offset % 2 = 1 ORDER BY b LIMIT 3 SETTINGS force_optimize_projection = 1;

-- `_part_offset` only filtered: lazy materialization is still used.
SELECT count() FROM (EXPLAIN SELECT a, b FROM t_proj2 WHERE b < 5000 AND _part_offset % 2 = 1 ORDER BY b LIMIT 3 SETTINGS force_optimize_projection = 1)
WHERE explain ILIKE '%LazilyReadFromMergeTree%';
SELECT a, b FROM t_proj2 WHERE b < 5000 AND _part_offset % 2 = 1 ORDER BY b LIMIT 3 SETTINGS force_optimize_projection = 1;

-- Table columns named like the virtual columns.
CREATE TABLE t_col (a Int32, b Int32, _part_offset UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_col SELECT number, (number * 7919) % 100000, 99999 - number FROM numbers(100000);
SELECT a, b, _part_offset FROM t_col WHERE b < 5000 ORDER BY b LIMIT 3;
SELECT a, b FROM t_col WHERE b < 5000 ORDER BY b LIMIT 3;

CREATE TABLE t_start (a Int32, b Int32, _part_starting_offset UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_start SELECT number, (number * 7919) % 100000, 1000 FROM numbers(100000);
SELECT a, b FROM t_start WHERE b < 5000 ORDER BY b LIMIT 3;

CREATE TABLE t_alias (a Int32, b Int32, _part_starting_offset UInt64 ALIAS toUInt64(a) + 7)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 64;
INSERT INTO t_alias SELECT number, (number * 7919) % 100000 FROM numbers(100000);
SELECT a, b FROM t_alias WHERE b < 5000 ORDER BY b LIMIT 3;

-- FINAL with intersecting and non-intersecting ranges, with and without a `_part_offset` column.
CREATE TABLE t_repl (k UInt64, v UInt64, payload String, flag UInt8, _part_offset UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY k SETTINGS index_granularity = 64;
SYSTEM STOP MERGES t_repl;
INSERT INTO t_repl SELECT number, 1, toString(number), number % 1000 = 0, 50 FROM numbers(100000);
INSERT INTO t_repl SELECT number * 1000, 2, concat('new', toString(number)), 1, 60 FROM numbers(20);
SELECT k, v, payload FROM t_repl FINAL WHERE flag = 1 ORDER BY k LIMIT 5;
SELECT count(), sum(v), sum(k) FROM (SELECT k, v FROM t_repl FINAL WHERE flag = 1)
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, query_plan_optimize_lazy_materialization = 0;

CREATE TABLE t_repl_plain (k UInt64, v UInt64, payload String, flag UInt8)
ENGINE = ReplacingMergeTree(v) ORDER BY k SETTINGS index_granularity = 64;
SYSTEM STOP MERGES t_repl_plain;
INSERT INTO t_repl_plain SELECT number, 1, toString(number), number % 1000 = 0 FROM numbers(100000);
INSERT INTO t_repl_plain SELECT number * 1000, 2, concat('new', toString(number)), 1 FROM numbers(20);
SELECT count() FROM (EXPLAIN SELECT count(), sum(v), sum(k) FROM (SELECT k, v FROM t_repl_plain FINAL WHERE flag = 1)
    SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, query_plan_optimize_lazy_materialization = 0)
WHERE explain ILIKE '%LazyReadReplacingFinal%';
SELECT count(), sum(v), sum(k) FROM (SELECT k, v FROM t_repl_plain FINAL WHERE flag = 1)
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, query_plan_optimize_lazy_materialization = 0;

DROP TABLE t_proj;
DROP TABLE t_proj2;
DROP TABLE t_col;
DROP TABLE t_start;
DROP TABLE t_alias;
DROP TABLE t_repl;
DROP TABLE t_repl_plain;
