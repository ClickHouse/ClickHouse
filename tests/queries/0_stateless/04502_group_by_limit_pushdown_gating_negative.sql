-- Tags: no-parallel-replicas, long, no-sanitizers
-- Gating and applicability of the GROUP BY top-K optimization, correctness at
-- the edges, negative cases, two-level hash-table conversion, huge limits, and
-- the Distinct-combinator use-after-destroy regression.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET enable_group_by_top_k_optimization = 1;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

SELECT 'external agg: no ORDER BY still applies';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k LIMIT 5
    SETTINGS max_bytes_ratio_before_external_group_by = 0.5
) WHERE explain LIKE '%Top-K%';

SELECT 'external agg: with ORDER BY still applies';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 5
    SETTINGS max_bytes_ratio_before_external_group_by = 0.5
) WHERE explain LIKE '%Top-K%';

SELECT 'huge limit: gated off';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 1000000000
    SETTINGS max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

SELECT 'limit at cap: applies';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 1000
    SETTINGS query_plan_max_limit_for_top_k_optimization = 1000, max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

SELECT 'limit above cap: gated off';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 1001
    SETTINGS query_plan_max_limit_for_top_k_optimization = 1000, max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

-- `enable_group_by_top_k_optimization` takes effect per query, not per
-- subquery: inside a single statement the last `SETTINGS` clause wins for the
-- whole query, so an on-versus-off comparison written as one `EXCEPT` or `JOIN`
-- runs both sides in the same mode and proves nothing.  Every comparison below
-- therefore materializes the unoptimized answer in its own statement first.
DROP TABLE IF EXISTS gt_nullable_eviction;
CREATE TABLE gt_nullable_eviction (k Nullable(String), c UInt64, s UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_eviction
SELECT k, count() AS c, sum(v) AS s FROM (
    SELECT if(number % 500 = 0, NULL, toNullable(toString(999999 - number))) AS k, number AS v FROM numbers(40000)
) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable key eviction: result matches optimization off';
SELECT count(), countIf(same) FROM (
    SELECT l.c = r.c AND l.s = r.s AS same FROM (
        SELECT k, count() AS c, sum(v) AS s FROM (
            SELECT if(number % 500 = 0, NULL, toNullable(toString(999999 - number))) AS k, number AS v FROM numbers(40000)
        ) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 10
    ) AS l
    INNER JOIN gt_nullable_eviction AS r ON l.k IS NOT DISTINCT FROM r.k
) SETTINGS max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, max_block_size = 4096;

SELECT 'non-prefix ORDER BY: not optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(1000)) GROUP BY a, b ORDER BY b LIMIT 5
) WHERE explain LIKE '%Top-K%';

SELECT 'reordered ORDER BY: not optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(1000)) GROUP BY a, b ORDER BY b, a LIMIT 5
) WHERE explain LIKE '%Top-K%';

DROP TABLE IF EXISTS gt_non_prefix_order_by;
CREATE TABLE gt_non_prefix_order_by (a UInt64, b UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_non_prefix_order_by
SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(20000)) GROUP BY a, b ORDER BY b, a LIMIT 5;
SET enable_group_by_top_k_optimization = 1;

SELECT 'non-prefix ORDER BY: result matches optimization off';
SELECT count() FROM (
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(20000)) GROUP BY a, b ORDER BY b, a LIMIT 5
) AS l
INNER JOIN gt_non_prefix_order_by AS r USING (a, b);

-- The GROUP BY top-K optimization cases that depend on how a colliding alias name
-- (`-k AS k` with `prefer_column_name_to_alias`) is resolved in `ORDER BY` live in
-- 04499_group_by_limit_pushdown_composite_prefix, which forces the analyzer for
-- them; the old analyzer resolves that construct inconsistently across query
-- contexts.

-- Edge cases, negative tests, and two-level hash-table conversion.

DROP TABLE IF EXISTS t_gbylimit_edge;

CREATE TABLE t_gbylimit_edge
(
    k_u32 UInt32,
    k_u64 UInt64,
    val UInt64
) ENGINE = MergeTree ORDER BY k_u64;

INSERT INTO t_gbylimit_edge
SELECT
    (number * 7 + 13) % 40000,
    number,
    number
FROM numbers(50000);

DROP TABLE IF EXISTS gt_limit_with_offset;
DROP TABLE IF EXISTS gt_multiple_aggregates;
DROP TABLE IF EXISTS gt_desc_order;
DROP TABLE IF EXISTS gt_with_totals;
DROP TABLE IF EXISTS gt_having;
DROP TABLE IF EXISTS gt_order_by_aggregate;
DROP TABLE IF EXISTS gt_multi_key;

CREATE TABLE gt_limit_with_offset (k_u64 UInt64, c UInt64, s UInt64) ENGINE = Memory;
CREATE TABLE gt_multiple_aggregates (k_u32 UInt32, c UInt64, s UInt64, mn UInt64, mx UInt64, av Float64) ENGINE = Memory;
CREATE TABLE gt_desc_order (k_u64 UInt64, c UInt64) ENGINE = Memory;
CREATE TABLE gt_with_totals (k_u32 UInt32, cnt UInt64) ENGINE = Memory;
CREATE TABLE gt_having (k_u32 UInt32, cnt UInt64) ENGINE = Memory;
CREATE TABLE gt_order_by_aggregate (k_u32 UInt32, cnt UInt64) ENGINE = Memory;
CREATE TABLE gt_multi_key (k_u32 UInt32, k_u64 UInt64, c UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;

INSERT INTO gt_limit_with_offset
SELECT k_u64, count(), sum(val) FROM t_gbylimit_edge GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10;
INSERT INTO gt_multiple_aggregates
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val) FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10;
INSERT INTO gt_desc_order
SELECT k_u64, count() FROM t_gbylimit_edge GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10;
INSERT INTO gt_with_totals
SELECT k_u32, count() AS cnt FROM t_gbylimit_edge GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10;
INSERT INTO gt_having
SELECT k_u32, count() AS cnt FROM t_gbylimit_edge GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10;
INSERT INTO gt_order_by_aggregate
SELECT k_u32, count() AS cnt FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10;
INSERT INTO gt_multi_key
SELECT k_u32, k_u64, count() FROM t_gbylimit_edge GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10;

SET enable_group_by_top_k_optimization = 1;

SELECT 'limit_with_offset';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit_edge GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
EXCEPT
SELECT * FROM gt_limit_with_offset;
SELECT * FROM (SELECT * FROM gt_limit_with_offset)
EXCEPT
SELECT * FROM (SELECT k_u64, count(), sum(val)
FROM t_gbylimit_edge GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10);

SELECT 'multiple_aggregates';
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_multiple_aggregates;
SELECT * FROM (SELECT * FROM gt_multiple_aggregates)
EXCEPT
SELECT * FROM (SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10);

SELECT 'desc_order';
SELECT k_u64, count()
FROM t_gbylimit_edge GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_desc_order;
SELECT * FROM (SELECT * FROM gt_desc_order)
EXCEPT
SELECT * FROM (SELECT k_u64, count()
FROM t_gbylimit_edge GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10);

SELECT 'negative_with_totals';
SELECT count() FROM (
    (
        SELECT k_u32, count() AS cnt
        FROM t_gbylimit_edge GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10
        EXCEPT
        SELECT * FROM gt_with_totals
    )
    UNION ALL
    (
        SELECT * FROM gt_with_totals
        EXCEPT
        SELECT k_u32, count() AS cnt
        FROM t_gbylimit_edge GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10
    )
);

SELECT 'negative_having';
SELECT k_u32, count() AS cnt
FROM t_gbylimit_edge GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_having;
SELECT * FROM (SELECT * FROM gt_having)
EXCEPT
SELECT * FROM (SELECT k_u32, count() AS cnt
FROM t_gbylimit_edge GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10);

SELECT 'negative_order_by_aggregate';
SELECT k_u32, count() AS cnt
FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_order_by_aggregate;
SELECT * FROM (SELECT * FROM gt_order_by_aggregate)
EXCEPT
SELECT * FROM (SELECT k_u32, count() AS cnt
FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10);

SELECT 'negative_multi_key';
SELECT k_u32, k_u64, count()
FROM t_gbylimit_edge GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_multi_key;
SELECT * FROM (SELECT * FROM gt_multi_key)
EXCEPT
SELECT * FROM (SELECT k_u32, k_u64, count()
FROM t_gbylimit_edge GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10);

SELECT 'gated shapes carry no Top-K in the plan, the control does';
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k_u32, count() FROM t_gbylimit_edge GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10);
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k_u32, count() FROM t_gbylimit_edge GROUP BY ROLLUP(k_u32) ORDER BY k_u32 ASC LIMIT 10);
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k_u32, count() FROM t_gbylimit_edge GROUP BY CUBE(k_u32) ORDER BY k_u32 ASC LIMIT 10);
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k_u32, count() FROM t_gbylimit_edge GROUP BY GROUPING SETS ((k_u32), (k_u64)) ORDER BY k_u32 ASC LIMIT 10);
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k_u32, count() FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10 WITH TIES);
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k_u32, count() FROM t_gbylimit_edge GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10);

-- CI randomizes group_by_two_level_threshold (can exceed the row count below);
-- pin it so the two-level conversion is deterministic, and keep the row count
-- moderate so a debug-build flaky-check run fits the timeout.
DROP TABLE IF EXISTS gt_two_level;
DROP TABLE IF EXISTS gt_two_level_string;
CREATE TABLE gt_two_level (number UInt64, c UInt64) ENGINE = Memory;
CREATE TABLE gt_two_level_string (k String, c UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_two_level
SELECT number, count() FROM numbers(600000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
INSERT INTO gt_two_level_string
SELECT toString(number) AS k, count() FROM numbers(600000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET enable_group_by_top_k_optimization = 1;

SELECT 'two_level';
SELECT number, count()
FROM numbers(600000) GROUP BY number ORDER BY number ASC LIMIT 10
EXCEPT
SELECT * FROM gt_two_level
SETTINGS group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SELECT * FROM (SELECT * FROM gt_two_level
SETTINGS group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000)
EXCEPT
SELECT * FROM (SELECT number, count()
FROM numbers(600000) GROUP BY number ORDER BY number ASC LIMIT 10);

SELECT 'two_level_string';
SELECT toString(number) AS k, count()
FROM numbers(600000) GROUP BY k ORDER BY k ASC LIMIT 10
EXCEPT
SELECT * FROM gt_two_level_string
SETTINGS group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SELECT * FROM (SELECT * FROM gt_two_level_string
SETTINGS group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000)
EXCEPT
SELECT * FROM (SELECT toString(number) AS k, count()
FROM numbers(600000) GROUP BY k ORDER BY k ASC LIMIT 10);

DROP TABLE t_gbylimit_edge;
DROP TABLE gt_limit_with_offset;
DROP TABLE gt_multiple_aggregates;
DROP TABLE gt_desc_order;
DROP TABLE gt_with_totals;
DROP TABLE gt_having;
DROP TABLE gt_order_by_aggregate;
DROP TABLE gt_multi_key;
DROP TABLE gt_two_level;
DROP TABLE gt_two_level_string;

-- A `LIMIT` near the `size_t` range must keep every group and match the
-- unoptimized result (it once hit "Too large size passed to allocator" from
-- reserving `1.5 * limit` rows).
-- The cap is pinned to 0 (uncapped) so the heap actually engages with the
-- huge limit instead of being gated off by `query_plan_max_limit_for_top_k_optimization`.

SELECT 'Single numeric key, huge limit';
SELECT k FROM (SELECT number % 10 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0;

SELECT 'Single numeric key, huge limit with huge offset';
SELECT k FROM (SELECT number % 10 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806, 10 SETTINGS enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0;

SELECT 'Composite key, huge limit';
SELECT a, b FROM (SELECT number % 5 AS a, number % 3 AS b FROM numbers(1000)) GROUP BY a, b ORDER BY a, b LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0;

SELECT 'String / serialized key, huge limit';
SELECT k FROM (SELECT toString(number % 10) AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0;

SELECT 'No ORDER BY, huge limit keeps all groups';
SELECT count() FROM (SELECT k FROM (SELECT number % 10 AS k FROM numbers(1000)) GROUP BY k LIMIT 9223372036854775806 SETTINGS enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0);

DROP TABLE IF EXISTS gt_huge_limit;
CREATE TABLE gt_huge_limit (k UInt32, s UInt64) ENGINE = Memory;

SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_huge_limit
SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k;
SET enable_group_by_top_k_optimization = 1;

SELECT 'Huge limit result matches the unoptimized aggregation';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt32(999 - (number % 1000)) AS k, 1 AS v FROM numbers(4000)) GROUP BY k ORDER BY k LIMIT 9223372036854775806) AS l
    INNER JOIN gt_huge_limit AS f USING (k)
) SETTINGS query_plan_max_limit_for_top_k_optimization = 0;

DROP TABLE gt_huge_limit;

-- Regression test: enable_group_by_top_k_optimization with the Distinct
-- combinator caused a use-after-destroy.
-- https://github.com/ClickHouse/ClickHouse/pull/96630

DROP TABLE IF EXISTS t_gbylimit_distinct;

CREATE TABLE t_gbylimit_distinct (k UInt32, val UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_gbylimit_distinct SELECT number % 1000, number FROM numbers(10000);

SELECT 'distinct_combinator';
SELECT k, skewSampDistinct(val)
FROM t_gbylimit_distinct
GROUP BY k
ORDER BY k
LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1;

DROP TABLE t_gbylimit_distinct;

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';

DROP TABLE gt_nullable_eviction;
DROP TABLE gt_non_prefix_order_by;
