-- Regression for https://github.com/ClickHouse/ClickHouse/issues/43144 (PR #108055).
-- The lossy floating-point conversion used by the `values`/insert path (issue #43144) must not
-- leak into `KeyCondition` primary-key pruning. A `Float64` primary key compared against
-- `toUInt64(-1)` (the maximum UInt64, which is not exactly representable in `Float64`) must not be
-- pruned using a rounded bound: `KeyCondition` would round the literal to static_cast<Float64>(max)
-- and build the stricter range `x > rounded`, skipping the mark that holds the matching row.

DROP TABLE IF EXISTS t_float_pk_prune;

CREATE TABLE t_float_pk_prune (x Float64) ENGINE = MergeTree ORDER BY x;

-- static_cast<Float64>(0xFFFFFFFFFFFFFFFF) == 1.8446744073709552e19, which under the accurate
-- comparison used at execution time is greater than the integer 18446744073709551615.
INSERT INTO t_float_pk_prune VALUES (toFloat64(toUInt64(-1)));

-- The stored row satisfies `x > toUInt64(-1)` and must be returned: the mark must not be pruned.
SELECT count() FROM t_float_pk_prune WHERE x > toUInt64(-1);
SELECT x FROM t_float_pk_prune WHERE x > toUInt64(-1);

-- Same with the old analyzer, which also goes through KeyCondition.
SELECT count() FROM t_float_pk_prune WHERE x > toUInt64(-1) SETTINGS allow_experimental_analyzer = 0;

DROP TABLE t_float_pk_prune;
