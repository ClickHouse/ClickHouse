-- Test `query_plan_derive_not_null_filter_at_read`: an `IS NOT NULL` filter added at a table read
-- for a column that a `JOIN` condition or a filter higher in the plan proves cannot be NULL.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_outer_join_to_inner_join_transitively = 1;
SET query_plan_derive_not_null_filter_at_read = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS mid;
DROP TABLE IF EXISTS mid_no_statistics;
DROP TABLE IF EXISTS sparse_nulls;
DROP TABLE IF EXISTS small;

CREATE TABLE fact (id UInt64, v UInt64, nv Nullable(UInt64) STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple()
    AS SELECT number % 20, number, if(number % 5 = 0, number, NULL) FROM numbers(100);

-- `dense` is declared Nullable but holds no NULL, so statistics say a filter on it rejects nothing.
CREATE TABLE mid
(
    id UInt64,
    val Nullable(UInt64) STATISTICS(basic),
    payload Nullable(UInt64) STATISTICS(basic),
    dense Nullable(UInt64) STATISTICS(basic)
) ENGINE = MergeTree ORDER BY tuple()
    AS SELECT number, if(number % 10 IN (1, 3), number % 10, NULL), if(number % 3 = 0, NULL, number), number % 5 FROM numbers(20);

CREATE TABLE mid_no_statistics (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = ''
    AS SELECT number, if(number % 10 IN (1, 3), number % 10, NULL) FROM numbers(20);

-- 2% of `val` is NULL, below the default threshold.
CREATE TABLE sparse_nulls (val Nullable(UInt64) STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple()
    AS SELECT if(number % 50 = 0, NULL, number % 5) FROM numbers(100);

CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 2 * number + 1 FROM numbers(2);

SELECT '-- The key of an enclosing join gets the filter.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE explain LIKE '%isNotNull(val)%';

SELECT '-- No filter is added when the setting is off.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS query_plan_derive_not_null_filter_at_read = 0
) WHERE explain LIKE '%isNotNull(val)%';

SELECT '-- An INNER JOIN proves its own key not NULL.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid AS m INNER JOIN small AS s ON m.val = s.val
    SETTINGS query_plan_convert_outer_join_to_inner_join_transitively = 0
) WHERE explain LIKE '%isNotNull(val)%';

SELECT '-- A column that is not Nullable at the read gets no filter.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid AS m INNER JOIN small AS s ON m.id = s.val
) WHERE explain LIKE '%isNotNull(id)%';

SELECT '-- A column whose statistics report no NULL gets no filter.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid AS m INNER JOIN small AS s ON m.dense = s.val
) WHERE explain LIKE '%isNotNull(dense)%';

SELECT '-- A NULL fraction below the threshold gets no filter.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM sparse_nulls AS m INNER JOIN small AS s ON m.val = s.val
) WHERE explain LIKE '%isNotNull(val)%';

SELECT '-- The same column gets one once the threshold is lowered below its NULL fraction.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM sparse_nulls AS m INNER JOIN small AS s ON m.val = s.val
    SETTINGS query_plan_derive_not_null_filter_at_read_min_null_ratio = 0.01
) WHERE explain LIKE '%isNotNull(val)%';

SELECT '-- A column without statistics gets no filter.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_no_statistics AS m INNER JOIN small AS s ON m.val = s.val
) WHERE explain LIKE '%isNotNull(val)%';

SELECT '-- A conjunct already sitting above the read that rejects NULL makes the derived one redundant.';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    WHERE f.nv > 3
) WHERE explain LIKE '%isNotNull(nv)%';

SELECT '-- The added filter does not change the result.';
SELECT count(), sum(f.v), sum(m.val), sum(m.payload)
FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filter_at_read = 0;

SELECT count(), sum(f.v), sum(m.val), sum(m.payload)
FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filter_at_read = 1;

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE mid_no_statistics;
DROP TABLE sparse_nulls;
DROP TABLE small;
