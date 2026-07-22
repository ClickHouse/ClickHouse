-- Tags: no-parallel-replicas, long
-- Correctness of enable_group_by_top_k_optimization for composite GROUP BY keys,
-- ORDER BY prefix matching, and projections that reuse a GROUP BY key's name for
-- a different expression.

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;

DROP TABLE IF EXISTS t_gbylimit_comp;

CREATE TABLE t_gbylimit_comp
(
    a UInt32,
    b UInt32,
    c String,
    d Nullable(UInt32),
    val UInt64
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_gbylimit_comp
SELECT
    (number % 500)::UInt32,
    (number % 200)::UInt32,
    toString(number % 300),
    if(number % 97 = 0, NULL, (number % 400)::UInt32),
    number
FROM numbers(20000);

SELECT 'composite_two_int';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_int_string';
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_three_keys';
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_nullable';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'prefix_one_of_two';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b ASC;

SELECT 'prefix_one_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b, c ASC;

SELECT 'prefix_two_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b, c ASC;

SELECT 'prefix_with_offset';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b ASC;

SELECT 'composite_two_level';
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'prefix_two_level';
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY x, y ASC
EXCEPT
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY x, y ASC;

SELECT 'nullable_nulls_first_asc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_nulls_last_asc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_nulls_first_desc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_nulls_last_desc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_nullable_nulls_first';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_nullable_nulls_last';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_agg_one_key';
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_agg_all_keys';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_agg_key_desc';
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a DESC, count() ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a DESC, count() ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_nullable_nulls_first';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_nullable_nulls_last';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST, count() DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_collate';
SELECT a, max(c), count()
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, max(c) ASC COLLATE 'en' LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, max(c), count()
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, max(c) ASC COLLATE 'en' LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'trailing_duplicate_key';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, a ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, a ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_comp;

-- A projection that reuses a GROUP BY key's name for a different expression
-- (e.g. `-k AS k` with `prefer_column_name_to_alias`).  The heap ranks by the
-- actual GROUP BY key, so the optimizer only matches an ORDER BY key against a
-- GROUP BY key when that key passes through the projection unchanged.  A plain
-- `ORDER BY <key>` still passes through (the user rename lives in the final
-- projection after LIMIT), so it must keep optimizing; a non-pass-through must
-- never produce a wrong top-N.  How the colliding name is resolved in ORDER BY
-- is only well-defined under the analyzer; the old analyzer resolves the same
-- construct inconsistently across query contexts.
SET enable_analyzer = 1;
SET max_threads = 1;
SET optimize_trivial_group_by_limit_query = 0;

SELECT 'aliased key passes through: still optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT -k AS k, count() FROM (SELECT number % 1000 AS k FROM numbers(1000)) GROUP BY k ORDER BY k ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1
) WHERE explain LIKE '%Top-K%';

SELECT 'aliased key, prefer_column_name_to_alias = 1: result matches optimization off';
SELECT count() FROM (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 0
) AS r USING (k, c);

SELECT 'aliased key, ORDER BY position: result matches optimization off';
SELECT count() FROM (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 0
) AS r USING (k, c);

SELECT 'order by longer than group by: still optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(1000)) GROUP BY k ORDER BY k ASC, c DESC LIMIT 5
) WHERE explain LIKE '%Top-K%';

SELECT 'non-key sort column before a key: not optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k1, k2, count() AS c FROM (SELECT number % 10 AS k1, number % 7 AS k2 FROM numbers(1000)) GROUP BY k1, k2 ORDER BY k1 ASC, c DESC LIMIT 5
) WHERE explain LIKE '%Top-K%';
