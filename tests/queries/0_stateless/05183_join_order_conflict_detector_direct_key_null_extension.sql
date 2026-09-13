-- The join-order conflict detectors (CD-A / CD-C) relax the assoc/asscom reordering rules when the
-- ON condition rejects nulls on the null-supplying relation. That premise holds only if an unmatched
-- outer-join row is padded with a top-level SQL NULL. A key type the nullability conversion cannot
-- wrap is padded with the type default instead (`[]` for Array), and Variant/Dynamic are padded with
-- an internal NULL; a join key comparison MATCHES both, so the ON condition rejects nothing and the
-- reassociation loses rows. The analysis was introduced by
-- https://github.com/ClickHouse/ClickHouse/pull/119305.
--
-- Every arm pins its own SETTINGS on purpose: clickhouse-test randomizes
-- query_plan_optimize_join_order_algorithm on every run and query_plan_optimize_join_order_limit to 0
-- in ~5% of runs, either of which would silently make the arm vacuous.
--
-- The plan arms print the ON condition of each join step, root first. `k = k` at the root is the
-- original left-deep association `(t1 LEFT JOIN t2) LEFT JOIN t3`; `a = a` at the root is the
-- right-deep reassociation the detectors unlock. Asserting the conditions rather than the step
-- sequence keeps the arm immune to the build/probe side swap, which is randomized.

SET enable_analyzer = 1, single_join_prefer_left_table = 0;
-- Pinned because the plan arms assert on the association the optimizer chooses.
SET query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- Make t1 look expensive so the detector prefers to reorder t2/t3 to the front.
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 100000, "distinct_keys": {"a": 2}}, "t2": {"cardinality": 1, "distinct_keys": {"a": 1, "k": 1}}, "t3": {"cardinality": 1, "distinct_keys": {"k": 1}}}';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (1), (2);

-- t1.a = 2 has no t2 match, so t2.k is null-extended. It must not match t3's null-ish key row, i.e.
-- the `2 \N null_key` row below must survive every reordering.

-- Array cannot be wrapped in Nullable, so the null extension writes `[]`, which matches t3's `[]`.
CREATE TABLE t2 (a Nullable(Int64), k Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k Array(Int64), tag String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2 VALUES (1, [10]);
INSERT INTO t3 VALUES ([10], 'ten'), ([], 'null_key');

SELECT 'Array    rows limit=0', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0;
SELECT 'Array    rows cd_a  ', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'Array    rows cd_c  ', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1;
SELECT 'Array    plan cd_a' AS arm, replaceRegexpOne(explain, '^[^A-Za-z]+', '') AS join_step
FROM ( EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1 )
WHERE explain LIKE '%Join conditions:%';

DROP TABLE t2;
DROP TABLE t3;

-- Variant is padded with a real NULL, but it is an internal (ColumnVariant) NULL that the join key
-- comparison treats as an ordinary key and matches against t3's NULL key.
CREATE TABLE t2 (a Nullable(Int64), k Variant(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k Variant(Int64), tag String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2 VALUES (1, 10::Int64);
INSERT INTO t3 VALUES (10::Int64, 'ten'), (NULL, 'null_key');

SELECT 'Variant  rows limit=0', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0;
SELECT 'Variant  rows cd_a  ', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'Variant  plan cd_a' AS arm, replaceRegexpOne(explain, '^[^A-Za-z]+', '') AS join_step
FROM ( EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1 )
WHERE explain LIKE '%Join conditions:%';

DROP TABLE t2;
DROP TABLE t3;

-- Control: a Nullable key IS padded with a top-level NULL that rejects the join condition, so the
-- reassociation is sound and must keep happening. This arm reddens if the fix over-restricts and
-- disables the detector instead of narrowing it.
CREATE TABLE t2 (a Nullable(Int64), k Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k Nullable(Int64), tag String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2 VALUES (1, 10);
INSERT INTO t3 VALUES (10, 'ten'), (NULL, 'null_key');

SELECT 'Nullable rows limit=0', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0;
SELECT 'Nullable rows cd_a  ', t1.a, t2.a, t3.tag FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1;
SELECT 'Nullable plan cd_a' AS arm, replaceRegexpOne(explain, '^[^A-Za-z]+', '') AS join_step
FROM ( EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_a = 1 )
WHERE explain LIKE '%Join conditions:%';

-- CD-C witness. CD-C must reach the same sound reassociation as CD-A; plain DPsub cannot reassociate
-- a LeftOuter/LeftOuter pair at all, so `a = a` at the root also proves the detector is engaged and
-- has not silently fallen back.
SELECT 'Nullable plan cd_c' AS arm, replaceRegexpOne(explain, '^[^A-Za-z]+', '') AS join_step
FROM ( EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k = t3.k
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_use_conflict_detector_c = 1 )
WHERE explain LIKE '%Join conditions:%';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
