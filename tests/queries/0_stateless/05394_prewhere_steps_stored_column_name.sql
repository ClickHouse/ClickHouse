-- Random settings limits: enable_multiple_prewhere_read_steps=(1, 1)
-- A stored column named like a condition of a PREWHERE split into steps returns its stored values.

CREATE TABLE t_mt (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, `greater(k, 5_UInt8)` String, `plus(x, 1_UInt8)` UInt16,
    `and(greater(x, 0_UInt8), less(x, 4_UInt8))` UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_mem (x UInt8, k UInt64, `greater(x, 1_UInt8)` UInt8, `greater(k, 5_UInt8)` String, `plus(x, 1_UInt8)` UInt16,
    `and(greater(x, 0_UInt8), less(x, 4_UInt8))` UInt8) ENGINE = Memory;
INSERT INTO t_mt VALUES (2, 10, 7, 'ten', 70, 17), (0, 20, 8, 'twenty', 80, 18), (3, 30, 9, 'thirty', 90, 19);
INSERT INTO t_mem SELECT * FROM t_mt;

SELECT x, `greater(x, 1_UInt8)` FROM t_mt PREWHERE x > 1 AND k > 5 ORDER BY x;
SELECT x, `greater(x, 1_UInt8)` FROM t_mem PREWHERE x > 1 AND k > 5 ORDER BY x;

-- A String column of that name.
SELECT x, `greater(k, 5_UInt8)` FROM t_mt PREWHERE k > 5 AND x > 1 ORDER BY x;
SELECT x, `greater(k, 5_UInt8)` FROM t_mem PREWHERE k > 5 AND x > 1 ORDER BY x;

-- An expression shared by two steps.
SELECT x, `plus(x, 1_UInt8)` FROM t_mt PREWHERE x + 1 > 2 AND (x + 1) * k > 5 ORDER BY x;

-- Two conditions of one step.
SELECT x, `and(greater(x, 0_UInt8), less(x, 4_UInt8))` FROM t_mt PREWHERE x > 0 AND x < 4 AND k > 5 ORDER BY x;

-- The guard still filters the rows a default expression or a conversion is evaluated on.
CREATE TABLE t_guard (gate UInt8, v String, `equals(gate, 1_UInt8)` UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_guard_mem (gate UInt8, v String, `equals(gate, 1_UInt8)` UInt8) ENGINE = Memory;
INSERT INTO t_guard VALUES (0, 'invalid', 3), (1, '42', 5);
INSERT INTO t_guard_mem SELECT * FROM t_guard;
ALTER TABLE t_guard ADD COLUMN n UInt64 DEFAULT toUInt64(v);
SELECT n, `equals(gate, 1_UInt8)` FROM t_guard PREWHERE gate = 1 AND n > 0;
SELECT toUInt64(v) AS u FROM t_guard PREWHERE gate = 1 WHERE u > 0
SETTINGS optimize_move_to_prewhere = 1, optimize_prewhere_after_pushdown = 1, move_all_conditions_to_prewhere = 1;
SELECT toUInt64(v) AS u FROM t_guard_mem PREWHERE gate = 1 WHERE u > 0
SETTINGS optimize_move_to_prewhere = 1, optimize_prewhere_after_pushdown = 1, move_all_conditions_to_prewhere = 1;

-- The column of the TopK dynamic filter.
CREATE TABLE t_topk (k UInt32, pred UInt32, `__topKFilter(k)` UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_topk SELECT number, number % 10, number * 37 % 251 FROM numbers(50000);
SELECT k, `__topKFilter(k)` AS x FROM t_topk PREWHERE pred = 3 ORDER BY k, x LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE t_mt;
DROP TABLE t_mem;
DROP TABLE t_guard;
DROP TABLE t_guard_mem;
DROP TABLE t_topk;
