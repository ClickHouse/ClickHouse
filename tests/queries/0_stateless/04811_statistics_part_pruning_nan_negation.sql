-- Statistics-based part pruning must not drop parts for negated predicates over floating-point
-- columns that contain NaN. Column min/max statistics are computed with `getExtremes`, which skips
-- NaN, so the stored range excludes NaN; yet NaN sorts after +inf and satisfies negated predicates
-- like NOT (f < c), f <> c and f NOT IN (...). The result with pruning on must match pruning off.

DROP TABLE IF EXISTS t;

CREATE TABLE t (k UInt64, f Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number, if(number < 13, nan, 1.5) FROM numbers(100000);
OPTIMIZE TABLE t FINAL; -- builds the column statistics for the merged part

-- NOT (f < c): NaN rows satisfy it (NaN < c is false), the 1.5 rows do not.
SELECT count() FROM t WHERE NOT (f < 101.5) SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t WHERE NOT (f < 101.5) SETTINGS use_statistics_for_part_pruning = 1;

-- f <> c (notEquals): NaN rows satisfy it, the 1.5 rows do not.
SELECT count() FROM t WHERE f <> 1.5 SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t WHERE f <> 1.5 SETTINGS use_statistics_for_part_pruning = 1;

-- f NOT IN (...) (notIn): NaN rows satisfy it, the 1.5 rows do not.
SELECT count() FROM t WHERE f NOT IN (1.5) SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t WHERE f NOT IN (1.5) SETTINGS use_statistics_for_part_pruning = 1;

-- Nested negation still reachable under the outer NOT.
SELECT count() FROM t WHERE NOT (f = 1.5 OR f > 2) SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t WHERE NOT (f = 1.5 OR f > 2) SETTINGS use_statistics_for_part_pruning = 1;

-- Non-negated predicate over the float column: NaN can never match, so pruning stays sound.
SELECT count() FROM t WHERE f > 1000000 SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t WHERE f > 1000000 SETTINGS use_statistics_for_part_pruning = 1;

DROP TABLE t;
