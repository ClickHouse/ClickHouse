-- The join-order conflict detectors reorder outer joins using null-rejection, and the reorderer
-- decides that by walking the ON expression through functions it believes propagate NULL. A CAST to
-- a NON-Nullable type does not propagate NULL: on a null-extended row it raises
-- CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN (349) instead of returning NULL. So such a CAST must not
-- count, and the plan for `CAST(t2.k AS Int64) = t3.k` must stay the plan the unoptimized query has.
-- Requested at https://github.com/ClickHouse/ClickHouse/pull/119305#discussion_r3990785321.
--
-- The plan arms read the Join/Read skeleton of the plan, in depth-first order: for a 3-way join
-- `Join, Join, Read, Read, Read` is left-deep and `Join, Read, Join, Read, Read` is right-deep.
-- They use EXPLAIN, so they cannot throw; the executed arms after them assert the exception the
-- plan choice exists to preserve. Two plan arms are non-vacuity controls: `cast-nullable cd_a` must
-- still reassociate (it reddens if every CAST is rejected instead of only the non-Nullable ones),
-- and `plain cd_a` must reassociate (it proves the detector is live and is what moves the plan).

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t3v;

CREATE TABLE t1 (a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (a Nullable(Int64), k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (1, 10);
INSERT INTO t3 VALUES (10);

-- A `Variant` (like a `Dynamic`) join key matches NULL to NULL, so an equality over one does not
-- reject a null-extended row: `Variant(Int64)` keys that are both NULL join, where `Nullable(Int64)`
-- keys that are both NULL do not. A cast to such a type therefore carries no null-rejection for the
-- reorderer to use, even though the cast itself returns NULL on a NULL input.
CREATE TABLE t3v (k Variant(Int64), tag String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t3v VALUES (10::Int64, 'ten'), (NULL, 'null_key');

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

-- A plan arm cannot see a change that keeps the left-deep skeleton and stops evaluating the cast on
-- the null-extended row, so the executed query is asserted too: the unoptimized query raises, and
-- under either detector it must still raise.
SELECT count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Int64) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Int64) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_algorithm = 'dpsub',
    query_plan_optimize_join_order_use_conflict_detector_a = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Int64) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_algorithm = 'dpsub',
    query_plan_optimize_join_order_use_conflict_detector_c = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- The reordering that stays permitted must not change the result.
SELECT 'rows cast-nullable noopt', count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Nullable(Int64)) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0;

SELECT 'rows cast-nullable cd_a ', count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON CAST(t2.k AS Nullable(Int64)) = t3.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_algorithm = 'dpsub',
    query_plan_optimize_join_order_use_conflict_detector_a = 1;

-- The null-extended `t1` row reaches `t3v`'s NULL key through the cast, so the reordering the
-- detector may not take here is the one that would drop that match. Both arms must agree.
SELECT 'rows variant  noopt', count(), countIf(t3v.tag = 'null_key'), countIf(t3v.tag = 'ten')
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3v ON CAST(t2.k AS Variant(Int64)) = t3v.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0;

SELECT 'rows variant  cd_a ', count(), countIf(t3v.tag = 'null_key'), countIf(t3v.tag = 'ten')
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3v ON CAST(t2.k AS Variant(Int64)) = t3v.k
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_algorithm = 'dpsub',
    query_plan_optimize_join_order_use_conflict_detector_a = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t3v;
