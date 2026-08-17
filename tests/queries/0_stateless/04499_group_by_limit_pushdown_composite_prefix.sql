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

-- `enable_group_by_top_k_optimization` takes effect per query, not per
-- subquery: inside a single statement the last `SETTINGS` clause wins for the
-- whole query, so an on-versus-off comparison written as one `EXCEPT` runs both
-- sides in the same mode and proves nothing.  Each comparison below therefore
-- materializes the unoptimized answer with its own statement first.
DROP TABLE IF EXISTS gt_composite_two_int;
CREATE TABLE gt_composite_two_int ENGINE = Memory EMPTY AS
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_two_int
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_two_int';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
EXCEPT
SELECT * FROM gt_composite_two_int;

DROP TABLE IF EXISTS gt_composite_int_string;
CREATE TABLE gt_composite_int_string ENGINE = Memory EMPTY AS
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_int_string
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_int_string';
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
EXCEPT
SELECT * FROM gt_composite_int_string;

DROP TABLE IF EXISTS gt_composite_three_keys;
CREATE TABLE gt_composite_three_keys ENGINE = Memory EMPTY AS
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_three_keys
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_three_keys';
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
EXCEPT
SELECT * FROM gt_composite_three_keys;

DROP TABLE IF EXISTS gt_composite_nullable;
CREATE TABLE gt_composite_nullable ENGINE = Memory EMPTY AS
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_nullable
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_nullable';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
EXCEPT
SELECT * FROM gt_composite_nullable;

DROP TABLE IF EXISTS gt_prefix_one_of_two;
CREATE TABLE gt_prefix_one_of_two ENGINE = Memory EMPTY AS
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_prefix_one_of_two
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'prefix_one_of_two';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM gt_prefix_one_of_two ORDER BY a, b ASC;

DROP TABLE IF EXISTS gt_prefix_one_of_three;
CREATE TABLE gt_prefix_one_of_three ENGINE = Memory EMPTY AS
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_prefix_one_of_three
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12;
SET enable_group_by_top_k_optimization = 1;

SELECT 'prefix_one_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM gt_prefix_one_of_three ORDER BY a, b, c ASC;

DROP TABLE IF EXISTS gt_prefix_two_of_three;
CREATE TABLE gt_prefix_two_of_three ENGINE = Memory EMPTY AS
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_prefix_two_of_three
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12;
SET enable_group_by_top_k_optimization = 1;

SELECT 'prefix_two_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM gt_prefix_two_of_three ORDER BY a, b, c ASC;

DROP TABLE IF EXISTS gt_prefix_with_offset;
CREATE TABLE gt_prefix_with_offset ENGINE = Memory EMPTY AS
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_prefix_with_offset
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6;
SET enable_group_by_top_k_optimization = 1;

SELECT 'prefix_with_offset';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM gt_prefix_with_offset ORDER BY a, b ASC;

DROP TABLE IF EXISTS gt_composite_two_level;
CREATE TABLE gt_composite_two_level ENGINE = Memory EMPTY AS
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_two_level
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_two_level';
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
EXCEPT
SELECT * FROM gt_composite_two_level;

DROP TABLE IF EXISTS gt_prefix_two_level;
CREATE TABLE gt_prefix_two_level ENGINE = Memory EMPTY AS
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_prefix_two_level
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'prefix_two_level';
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10
) ORDER BY x, y ASC
EXCEPT
SELECT * FROM gt_prefix_two_level ORDER BY x, y ASC;

DROP TABLE IF EXISTS gt_nullable_nulls_first_asc;
CREATE TABLE gt_nullable_nulls_first_asc ENGINE = Memory EMPTY AS
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_nulls_first_asc
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable_nulls_first_asc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5
EXCEPT
SELECT * FROM gt_nullable_nulls_first_asc;

DROP TABLE IF EXISTS gt_nullable_nulls_last_asc;
CREATE TABLE gt_nullable_nulls_last_asc ENGINE = Memory EMPTY AS
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_nulls_last_asc
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable_nulls_last_asc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5
EXCEPT
SELECT * FROM gt_nullable_nulls_last_asc;

DROP TABLE IF EXISTS gt_nullable_nulls_first_desc;
CREATE TABLE gt_nullable_nulls_first_desc ENGINE = Memory EMPTY AS
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_nulls_first_desc
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable_nulls_first_desc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5
EXCEPT
SELECT * FROM gt_nullable_nulls_first_desc;

DROP TABLE IF EXISTS gt_nullable_nulls_last_desc;
CREATE TABLE gt_nullable_nulls_last_desc ENGINE = Memory EMPTY AS
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_nulls_last_desc
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable_nulls_last_desc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5
EXCEPT
SELECT * FROM gt_nullable_nulls_last_desc;

DROP TABLE IF EXISTS gt_composite_nullable_nulls_first;
CREATE TABLE gt_composite_nullable_nulls_first ENGINE = Memory EMPTY AS
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_nullable_nulls_first
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_nullable_nulls_first';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10
EXCEPT
SELECT * FROM gt_composite_nullable_nulls_first;

DROP TABLE IF EXISTS gt_composite_nullable_nulls_last;
CREATE TABLE gt_composite_nullable_nulls_last ENGINE = Memory EMPTY AS
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_nullable_nulls_last
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite_nullable_nulls_last';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10
EXCEPT
SELECT * FROM gt_composite_nullable_nulls_last;

DROP TABLE IF EXISTS gt_trailing_agg_one_key;
CREATE TABLE gt_trailing_agg_one_key ENGINE = Memory EMPTY AS
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_agg_one_key
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_agg_one_key';
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, count() DESC LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_agg_one_key;

DROP TABLE IF EXISTS gt_trailing_agg_all_keys;
CREATE TABLE gt_trailing_agg_all_keys ENGINE = Memory EMPTY AS
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_agg_all_keys
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_agg_all_keys';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, count() DESC LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_agg_all_keys;

DROP TABLE IF EXISTS gt_trailing_agg_key_desc;
CREATE TABLE gt_trailing_agg_key_desc ENGINE = Memory EMPTY AS
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a DESC, count() ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_agg_key_desc
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a DESC, count() ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_agg_key_desc';
SELECT a, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a ORDER BY a DESC, count() ASC LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_agg_key_desc;

DROP TABLE IF EXISTS gt_trailing_nullable_nulls_first;
CREATE TABLE gt_trailing_nullable_nulls_first ENGINE = Memory EMPTY AS
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_nullable_nulls_first
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_nullable_nulls_first';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST, count() DESC LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_nullable_nulls_first;

DROP TABLE IF EXISTS gt_trailing_nullable_nulls_last;
CREATE TABLE gt_trailing_nullable_nulls_last ENGINE = Memory EMPTY AS
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_nullable_nulls_last
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST, count() DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_nullable_nulls_last';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST, count() DESC LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_nullable_nulls_last;

DROP TABLE IF EXISTS gt_trailing_collate;
CREATE TABLE gt_trailing_collate ENGINE = Memory EMPTY AS
SELECT a, max(c), count()
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, max(c) ASC COLLATE 'en' LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_collate
SELECT a, max(c), count()
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, max(c) ASC COLLATE 'en' LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_collate';
SELECT a, max(c), count()
FROM t_gbylimit_comp GROUP BY a ORDER BY a ASC, max(c) ASC COLLATE 'en' LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_collate;

DROP TABLE IF EXISTS gt_trailing_duplicate_key;
CREATE TABLE gt_trailing_duplicate_key ENGINE = Memory EMPTY AS
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, a ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_trailing_duplicate_key
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, a ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'trailing_duplicate_key';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC, b ASC, a ASC LIMIT 10
EXCEPT
SELECT * FROM gt_trailing_duplicate_key;

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

DROP TABLE IF EXISTS gt_aliased_key;
CREATE TABLE gt_aliased_key ENGINE = Memory EMPTY AS
SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS prefer_column_name_to_alias = 1;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_aliased_key
SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS prefer_column_name_to_alias = 1;
SET enable_group_by_top_k_optimization = 1;

SELECT 'aliased key, prefer_column_name_to_alias = 1: result matches optimization off';
SELECT count() FROM (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
) AS l
INNER JOIN gt_aliased_key AS r USING (k, c)
SETTINGS prefer_column_name_to_alias = 1;

DROP TABLE IF EXISTS gt_aliased_key_order_by_position;
CREATE TABLE gt_aliased_key_order_by_position ENGINE = Memory EMPTY AS
SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
SETTINGS prefer_column_name_to_alias = 1;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_aliased_key_order_by_position
SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
SETTINGS prefer_column_name_to_alias = 1;
SET enable_group_by_top_k_optimization = 1;

SELECT 'aliased key, ORDER BY position: result matches optimization off';
SELECT count() FROM (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
) AS l
INNER JOIN gt_aliased_key_order_by_position AS r USING (k, c)
SETTINGS prefer_column_name_to_alias = 1;

SELECT 'order by longer than group by: still optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(1000)) GROUP BY k ORDER BY k ASC, c DESC LIMIT 5
) WHERE explain LIKE '%Top-K%';

SELECT 'non-key sort column before a key: not optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k1, k2, count() AS c FROM (SELECT number % 10 AS k1, number % 7 AS k2 FROM numbers(1000)) GROUP BY k1, k2 ORDER BY k1 ASC, c DESC LIMIT 5
) WHERE explain LIKE '%Top-K%';

DROP TABLE gt_prefix_one_of_two;
DROP TABLE gt_prefix_one_of_three;
DROP TABLE gt_prefix_two_of_three;
DROP TABLE gt_prefix_with_offset;
DROP TABLE gt_prefix_two_level;
DROP TABLE gt_composite_two_int;
DROP TABLE gt_composite_int_string;
DROP TABLE gt_composite_three_keys;
DROP TABLE gt_composite_nullable;
DROP TABLE gt_composite_two_level;
DROP TABLE gt_nullable_nulls_first_asc;
DROP TABLE gt_nullable_nulls_last_asc;
DROP TABLE gt_nullable_nulls_first_desc;
DROP TABLE gt_nullable_nulls_last_desc;
DROP TABLE gt_composite_nullable_nulls_first;
DROP TABLE gt_composite_nullable_nulls_last;
DROP TABLE gt_trailing_agg_one_key;
DROP TABLE gt_trailing_agg_all_keys;
DROP TABLE gt_trailing_agg_key_desc;
DROP TABLE gt_trailing_nullable_nulls_first;
DROP TABLE gt_trailing_nullable_nulls_last;
DROP TABLE gt_trailing_collate;
DROP TABLE gt_trailing_duplicate_key;
DROP TABLE gt_aliased_key;
DROP TABLE gt_aliased_key_order_by_position;
