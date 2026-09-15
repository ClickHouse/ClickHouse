-- The plan assertions below match analyzer-generated column identifiers (`__table2.`) in the
-- `EXPLAIN` output, so the analyzer is pinned for the whole file. The old analyzer does not build the
-- plan shape that triggers this bug, so nothing is lost by pinning.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t2_string;
DROP TABLE IF EXISTS t2_lc;
DROP TABLE IF EXISTS t2_uint8;
DROP TABLE IF EXISTS t2_lc_nullable;
DROP TABLE IF EXISTS t2_nullable;
DROP TABLE IF EXISTS mt1;
DROP TABLE IF EXISTS mt2;

CREATE TABLE t1 (id Int64, grp Int64) ENGINE = Memory;
CREATE TABLE t2 (id Int64, reviewer Int64, enabled Bool) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 10), (2, 20);
INSERT INTO t2 VALUES (1, 100, true), (2, 200, false);

SELECT 'left join, right-side ON condition repeated in WHERE';
SELECT t1.id, t2.reviewer
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.enabled = true
WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- The `AND`-chain split has to rename the colliding boundary input. Assert the rename is in the
-- plan: a plan shape that never reaches the split would make every case below pass vacuously.
-- The extra settings are pinned (all randomized in CI) because the assertion needs both colliding
-- atoms to stay on one shared filter step below the join.
SELECT 'the colliding AND atom is renamed';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1, pretty = 0
    SELECT t1.id, t2.reviewer
    FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.enabled = true
    WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
    SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_remove_unused_columns = 1, query_plan_merge_filter_into_join_condition = 0,
             query_plan_optimize_join_order_randomize = 0, optimize_move_to_prewhere = 0,
             query_plan_optimize_prewhere = 0
) WHERE position(explain, 'AND column: equals(__table2.enabled, 1_Bool)_0') > 0;

-- The rename above is printed by `FilterStep::describeActions`, which splits a clone of the DAG and
-- never builds a pipeline. Assert the `AND` chain is also split on the real execution path. One
-- `FilterTransform` is emitted per extracted atom plus one for the remainder, so a multiplicity of 3
-- means two atoms were extracted: the collision this fixes needs that second, iterated split.
-- `max_threads` is pinned so the multiplicity counts split atoms, not parallel streams.
SELECT 'the AND chain is split at runtime';
SELECT max(toUInt32OrZero(extract(explain, 'FilterTransform[^0-9]+([0-9]+)'))) >= 3 FROM (
    EXPLAIN PIPELINE
    SELECT t1.id, t2.reviewer
    FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.enabled = true
    WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
    SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_remove_unused_columns = 1, query_plan_merge_filter_into_join_condition = 0,
             query_plan_optimize_join_order_randomize = 0, optimize_move_to_prewhere = 0,
             query_plan_optimize_prewhere = 0, max_threads = 1
);

SELECT 'right join, left-side ON condition repeated in WHERE';
SELECT t1.id, t2.reviewer
FROM t2 RIGHT JOIN t1 ON t1.id = t2.id AND t1.grp = 10
WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

SELECT 'full join';
SELECT t1.id, t2.reviewer
FROM t1 FULL JOIN t2 ON t1.id = t2.id AND t2.enabled = true
WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

SELECT 'inner join';
SELECT t1.id, t2.reviewer
FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t2.enabled = true
WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

SELECT 'left join, non-Bool repeated condition';
SELECT t1.id, t2.reviewer
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.reviewer = 100
WHERE t1.grp = 10 AND t2.enabled = true AND t2.reviewer = 100
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- Two further affected shapes: with an `ARRAY JOIN` above the join, and with `ORDER BY` on an
-- expression. Both throw the same 352 on master and return the correct result with the fix.
SELECT 'left join, ARRAY JOIN above the join';
SELECT t1.id, t2.reviewer, x
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.enabled = true
ARRAY JOIN [1, 2] AS x
WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
ORDER BY t1.id, x
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

SELECT 'left join, ORDER BY an expression above the join';
SELECT t1.id, t2.reviewer
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.enabled = true
WHERE t1.grp = 10 AND t2.reviewer = 100 AND t2.enabled = true
ORDER BY t2.reviewer + 1
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

CREATE TABLE t2_string (id Int64, reviewer Int64, tag String) ENGINE = Memory;
INSERT INTO t2_string VALUES (1, 100, 'x'), (2, 200, 'y');

SELECT 'left join, String repeated condition';
SELECT t1.id, t2_string.reviewer
FROM t1 LEFT JOIN t2_string ON t1.id = t2_string.id AND t2_string.tag = 'x'
WHERE t1.grp = 10 AND t2_string.reviewer = 100 AND t2_string.tag = 'x'
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

CREATE TABLE t2_lc (id Int64, reviewer Int64, tag LowCardinality(String)) ENGINE = Memory;
INSERT INTO t2_lc VALUES (1, 100, 'x'), (2, 200, 'y');

SELECT 'left join, LowCardinality repeated condition';
SELECT t1.id, t2_lc.reviewer
FROM t1 LEFT JOIN t2_lc ON t1.id = t2_lc.id AND t2_lc.tag = 'x'
WHERE t1.grp = 10 AND t2_lc.reviewer = 100 AND t2_lc.tag = 'x'
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- `Bool` is a `UInt8` domain (a display name over `UInt8`), so it does not cover a plain `UInt8`
-- column: the colliding name carries the type spelling (`1_Bool` vs `1_UInt8`). Pin both.
CREATE TABLE t2_uint8 (id Int64, reviewer Int64, flag UInt8) ENGINE = Memory;
INSERT INTO t2_uint8 VALUES (1, 100, 1), (2, 200, 0);

SELECT 'left join, plain UInt8 repeated condition';
SELECT t1.id, t2_uint8.reviewer
FROM t1 LEFT JOIN t2_uint8 ON t1.id = t2_uint8.id AND t2_uint8.flag = 1
WHERE t1.grp = 10 AND t2_uint8.reviewer = 100 AND t2_uint8.flag = 1
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

CREATE TABLE t2_nullable (id Int64, reviewer Int64, enabled Nullable(Bool)) ENGINE = Memory;
INSERT INTO t2_nullable VALUES (1, 100, true), (2, 200, false), (3, 300, NULL);

SELECT 'left join, already Nullable repeated condition (non-regression)';
SELECT t1.id, t2_nullable.reviewer
FROM t1 LEFT JOIN t2_nullable ON t1.id = t2_nullable.id AND t2_nullable.enabled = true
WHERE t1.grp = 10 AND t2_nullable.reviewer = 100 AND t2_nullable.enabled = true
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- Also non-regression, for the same reason as the case above: `makeNullableOrLowCardinalityNullable`
-- early-returns on `isLowCardinalityNullable`, so promoting this column is a no-op and the `ON` and
-- `WHERE` copies of the predicate are computed on the identical type. Kept as the negative half of
-- the `LowCardinality` wrapper matrix.
CREATE TABLE t2_lc_nullable (id Int64, reviewer Int64, tag LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO t2_lc_nullable VALUES (1, 100, 'x'), (2, 200, NULL);

SELECT 'left join, LowCardinality(Nullable) repeated condition (no promotion, non-regression)';
SELECT t1.id, t2_lc_nullable.reviewer
FROM t1 LEFT JOIN t2_lc_nullable ON t1.id = t2_lc_nullable.id AND t2_lc_nullable.tag = 'x'
WHERE t1.grp = 10 AND t2_lc_nullable.reviewer = 100 AND t2_lc_nullable.tag = 'x'
ORDER BY t1.id
SETTINGS join_use_nulls = 1, query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

CREATE TABLE mt1 (id Int64, grp Int64) ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
CREATE TABLE mt2 (id Int64, reviewer Int64, enabled Bool) ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO mt1 VALUES (1, 10), (2, 20);
INSERT INTO mt2 VALUES (1, 100, true), (2, 200, false);

SELECT 'left join over ReplacingMergeTree with FINAL, prewhere disabled';
SELECT mt1.id, mt2.reviewer
FROM mt1 LEFT JOIN mt2 ON mt1.id = mt2.id AND mt2.enabled = true
WHERE mt1.grp = 10 AND mt2.reviewer = 100 AND mt2.enabled = true
ORDER BY mt1.id
SETTINGS join_use_nulls = 1, final = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         query_plan_merge_filters = 1, query_plan_convert_outer_join_to_inner_join = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t2_string;
DROP TABLE t2_lc;
DROP TABLE t2_uint8;
DROP TABLE t2_lc_nullable;
DROP TABLE t2_nullable;
DROP TABLE mt1;
DROP TABLE mt2;
