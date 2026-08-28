-- Tags: no-random-settings
-- Test the promotion of NOT NULL planner-only filters derived from joins to executable filters.

SET max_threads = 2;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET materialize_statistics_on_insert = 1;
SET query_plan_max_selectivity_for_not_null_filters_execution = 0.5;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS mid_hi;
DROP TABLE IF EXISTS mid_lo;
DROP TABLE IF EXISTS mid_nn;
DROP TABLE IF EXISTS mid_two;
DROP TABLE IF EXISTS small;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 100, number FROM numbers(1000);

CREATE TABLE mid_hi (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 10 < 6, NULL, number % 20) FROM numbers(100); -- 60% NULLs

CREATE TABLE mid_lo (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 10 = 0, NULL, number % 20) FROM numbers(100); -- 10% NULLs

CREATE TABLE mid_nn (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, number % 20 FROM numbers(100);

CREATE TABLE mid_two (id UInt64, val1 Nullable(UInt64), val2 Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS
    SELECT number, if(number % 5 < 3, NULL, number % 20), if(number % 10 < 6, NULL, number % 20) FROM numbers(100); -- 60% NULLs each

CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number FROM numbers(20);

SELECT '-- The derived NOT NULL filter on a natively Nullable column with a high fraction of NULLs is promoted to an executable filter.';
SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_allow_derived_not_null_filters_execution = 0;

SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- Promotion is disabled, no filter is applied even with a high fraction of NULLs.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val
    SETTINGS query_plan_allow_derived_not_null_filters_execution = 0
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- A user predicate on the same column already rejects NULLs: the derived filter is subsumed and dropped.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val WHERE m.val >= 6
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- A user predicate on a different column cannot subsume the derived filter, it is still promoted.';
SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val WHERE m.id < 90
SETTINGS query_plan_allow_derived_not_null_filters_execution = 0;

SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val WHERE m.id < 90;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_hi AS m INNER JOIN small AS s ON m.val = s.val WHERE m.id < 90
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- A low fraction of NULLs does not clear the selectivity gate, no filter is applied.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_lo AS m INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- Increasing the maximum selectivity allows filters over columns with a low fraction of NULLs.';
SELECT count() FROM mid_lo AS m INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_allow_derived_not_null_filters_execution = 0;

SELECT count() FROM mid_lo AS m INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_max_selectivity_for_not_null_filters_execution = 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_lo AS m INNER JOIN small AS s ON m.val = s.val
    SETTINGS query_plan_max_selectivity_for_not_null_filters_execution = 1
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- Two joins on different Nullable columns, both derived filters are promoted.';
SELECT count() FROM mid_two AS m INNER JOIN small AS sa ON m.val1 = sa.val INNER JOIN small AS sb ON m.val2 = sb.val
SETTINGS query_plan_allow_derived_not_null_filters_execution = 0;

SELECT count() FROM mid_two AS m INNER JOIN small AS sa ON m.val1 = sa.val INNER JOIN small AS sb ON m.val2 = sb.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_two AS m INNER JOIN small AS sa ON m.val1 = sa.val INNER JOIN small AS sb ON m.val2 = sb.val
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- Two joins on the same column derive duplicate filters, only one is promoted.';
SELECT count() FROM mid_hi AS m INNER JOIN small AS sa ON m.val = sa.val INNER JOIN small AS sb ON m.val = sb.val
SETTINGS query_plan_allow_derived_not_null_filters_execution = 0;

SELECT count() FROM mid_hi AS m INNER JOIN small AS sa ON m.val = sa.val INNER JOIN small AS sb ON m.val = sb.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM mid_hi AS m INNER JOIN small AS sa ON m.val = sa.val INNER JOIN small AS sb ON m.val = sb.val
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- The filter derived at the enclosing join travels through the converted outer join to the scan and is promoted.';
SELECT count() FROM fact AS f LEFT JOIN mid_hi AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_allow_derived_not_null_filters_execution = 0;

SELECT count() FROM fact AS f LEFT JOIN mid_hi AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid_hi AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

SELECT '-- The filter derived over a non-Nullable column when join_use_nulls is true does not have a filtering effect, no filter is applied.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid_nn AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS join_use_nulls = 1
) WHERE trim(explain) LIKE 'Prewhere filter column:%' OR trim(explain) LIKE 'Filter column:%';

DROP TABLE fact;
DROP TABLE mid_hi;
DROP TABLE mid_lo;
DROP TABLE mid_nn;
DROP TABLE mid_two;
DROP TABLE small;
