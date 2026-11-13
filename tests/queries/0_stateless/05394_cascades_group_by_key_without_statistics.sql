-- A Cascades read adds an entry to the column statistics of every column it reads, to carry the
-- column width, whether the column has statistics or not. `GROUP BY` and `DISTINCT` took the zero
-- NDV of such an entry as known and estimated a single group with a maximum of zero rows. A key
-- without a known NDV now falls back to 10% of the input rows: 1000 of 10000 here.

DROP TABLE IF EXISTS t_no_stats;
DROP TABLE IF EXISTS t_known_ndv;

-- No statistics at all.
CREATE TABLE t_no_stats (k UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '';
INSERT INTO t_no_stats SELECT number % 100, if(number % 10 = 0, NULL, number % 100) FROM numbers(10000);

-- A matching input with a known NDV, supplied by the pinned hint below.
CREATE TABLE t_known_ndv (k UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '';
INSERT INTO t_known_ndv SELECT number % 10 FROM numbers(10000);

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_no_stats": {"cardinality": 10000}, "t_known_ndv": {"cardinality": 10000, "distinct_keys": {"k": 10}}}';

-- The outer probe queries turn distributed planning off because they read the plan through
-- `viewExplain`, which Cascades rejects; the explained queries turn it back on.
SELECT 'group by, no statistics:', countIf(explain LIKE '%Aggregating%(rows: ~1000.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT k, count() FROM t_no_stats GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT 'distinct, no statistics:', countIf(explain LIKE '%Distinct%(rows: ~1000.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT DISTINCT k FROM t_no_stats
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- An intersection's value domain is bounded by every input. Preserve a known NDV of 10 regardless
-- of operand order when the other input has only a width entry (NDV zero).
SELECT 'intersect, width-only first:', countIf(explain LIKE '%Distinct%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT count() FROM (
        SELECT k FROM t_no_stats INTERSECT DISTINCT SELECT k FROM t_known_ndv)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT 'intersect, width-only second:', countIf(explain LIKE '%Distinct%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT count() FROM (
        SELECT k FROM t_known_ndv INTERSECT DISTINCT SELECT k FROM t_no_stats)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- A multi-argument expression has no column-statistics entry. The positional bound from the other
-- operand still applies, again independently of operand order.
SELECT 'intersect, absent first:', countIf(explain LIKE '%Distinct%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT count() FROM (
        SELECT k + assumeNotNull(n) AS x FROM t_no_stats INTERSECT DISTINCT SELECT k AS x FROM t_known_ndv)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT 'intersect, absent second:', countIf(explain LIKE '%Distinct%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT count() FROM (
        SELECT k AS x FROM t_known_ndv INTERSECT DISTINCT SELECT k + assumeNotNull(n) AS x FROM t_no_stats)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- `assumeNotNull` maps NULL to one more value, which adds one to a known NDV. Added to the unknown
-- zero, it made the key look like a known single value.
SELECT 'group by assumeNotNull, no statistics:', countIf(explain LIKE '%Aggregating%(rows: ~1000.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT assumeNotNull(n) AS m, count() FROM t_no_stats GROUP BY m
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- A join took the zero NDV of a key without statistics as known. Narrowing the join keys to the smaller
-- NDV then erased the known NDV of 10 of the other side, and a `GROUP BY` on that key fell back to
-- 10% of the join output. A key without a known NDV is bounded by the row count of its side instead.
SELECT 'group by joined key, other side without statistics:', countIf(explain LIKE '%Aggregating%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT r.k, count() FROM t_no_stats AS l LEFT JOIN t_known_ndv AS r ON l.k = r.k GROUP BY r.k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_no_stats;
DROP TABLE t_known_ndv;
