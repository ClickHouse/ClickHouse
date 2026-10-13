-- Random settings limits: enable_multiple_prewhere_read_steps=(1, 1)
-- A stored column named like a condition of a PREWHERE split into steps returns its stored values.

CREATE TABLE t_mt (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, `greater(k, 5_UInt8)` String, `plus(x, 1_UInt8)` UInt16)
    ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_mem (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, `greater(k, 5_UInt8)` String, `plus(x, 1_UInt8)` UInt16)
    ENGINE = Memory;
INSERT INTO t_mt VALUES (2, 10, 7, 'ten', 70), (0, 20, 8, 'twenty', 80), (3, 30, 9, 'thirty', 90);
INSERT INTO t_mem SELECT * FROM t_mt;

SELECT x, `greater(x, 1_UInt8)` FROM t_mt PREWHERE x > 1 AND k > 5 ORDER BY x;
SELECT x, `greater(x, 1_UInt8)` FROM t_mem PREWHERE x > 1 AND k > 5 ORDER BY x;

-- A String column of that name.
SELECT x, `greater(k, 5_UInt8)` FROM t_mt PREWHERE k > 5 AND x > 1 ORDER BY x;
SELECT x, `greater(k, 5_UInt8)` FROM t_mem PREWHERE k > 5 AND x > 1 ORDER BY x;

-- An expression shared by two steps.
SELECT x, `plus(x, 1_UInt8)` FROM t_mt PREWHERE x + 1 > 2 AND (x + 1) * k > 5 ORDER BY x;

-- A projection index whose projection stores such a column.
CREATE TABLE t_proj (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, v UInt64,
    PROJECTION p (SELECT x, k, `greater(x, 1_UInt8)`, _part_offset ORDER BY x))
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_proj SELECT number % 4, number, (number * 7) % 251, number FROM numbers(100000);
SELECT count(), sum(v) FROM t_proj PREWHERE x > 1 WHERE k > 5
SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0,
    optimize_move_to_prewhere = 0;

-- The column of the TopK dynamic filter.
CREATE TABLE t_topk (k UInt32, pred UInt32, `__topKFilter(k)` UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk SELECT number, number % 10, number * 37 % 251 FROM numbers(50000);
SELECT k, `__topKFilter(k)` AS x FROM t_topk PREWHERE pred = 3 ORDER BY k, x LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE t_mt;
DROP TABLE t_mem;
DROP TABLE t_proj;
DROP TABLE t_topk;
