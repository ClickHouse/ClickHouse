-- The join-order conflict detectors reorder outer joins using null-rejection, and the reorderer
-- decides that by walking the ON expression through functions it believes propagate NULL. A CAST to
-- a NON-Nullable type does not propagate NULL: on a null-extended row it raises
-- CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN (349) instead of returning NULL. So such a CAST must not
-- count, and the plan for `CAST(t2.k AS Int64) = t3.k` must stay the plan the unoptimized query has.
-- Requested at https://github.com/ClickHouse/ClickHouse/pull/119305#discussion_r3990785321.
--
-- The oracle is the Join/Read skeleton of the plan, in depth-first order: for a 3-way join
-- `Join, Join, Read, Read, Read` is left-deep and `Join, Read, Join, Read, Read` is right-deep.
-- EXPLAIN only, so no arm can throw. Two arms are non-vacuity controls: `cast-nullable cd_a` must
-- still reassociate (it reddens if every CAST is rejected instead of only the non-Nullable ones),
-- and `plain cd_a` must reassociate (it proves the detector is live and is what moves the plan).

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (a Nullable(Int64), k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (1, 10);
INSERT INTO t3 VALUES (10);

SET enable_analyzer = 1, single_join_prefer_left_table = 0;
-- Pinned because the test asserts on the join plan shape.
SET query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- Make t1 look expensive so the detector prefers to reorder t2/t3 to the front.
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 100000, "distinct_keys": {"a": 2}}, "t2": {"cardinality": 1, "distinct_keys": {"a": 1, "k": 1}}, "t3": {"cardinality": 1, "distinct_keys": {"k": 1}}}';

SELECT 'baseline           ' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT 'cast-int64    cd_a ' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Int64) = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
            query_plan_optimize_join_order_algorithm = 'dpsub',
            query_plan_optimize_join_order_use_conflict_detector_a = 1
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT 'cast-int64    cd_c ' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Int64) = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
            query_plan_optimize_join_order_algorithm = 'dpsub',
            query_plan_optimize_join_order_use_conflict_detector_c = 1
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT 'cast-nullable cd_a ' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Nullable(Int64)) = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
            query_plan_optimize_join_order_algorithm = 'dpsub',
            query_plan_optimize_join_order_use_conflict_detector_a = 1
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT 'plain         cd_a ' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
            query_plan_optimize_join_order_algorithm = 'dpsub',
            query_plan_optimize_join_order_use_conflict_detector_a = 1
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

-- The reordering that stays permitted must not change the result.
SELECT 'rows cast-nullable noopt', count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Nullable(Int64)) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0;

SELECT 'rows cast-nullable cd_a ', count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Nullable(Int64)) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_algorithm = 'dpsub',
    query_plan_optimize_join_order_use_conflict_detector_a = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
