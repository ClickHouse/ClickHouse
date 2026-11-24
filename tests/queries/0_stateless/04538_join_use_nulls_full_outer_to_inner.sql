-- Conversion of outer joins to inner joins with join_use_nulls = 1.
-- With join_use_nulls the filter sees nullable join keys, so a condition like `l.k = 42 AND r.k = 42`
-- evaluates to `NULL AND <unknown>` for not-matched rows, which is falsy but does not fold to a constant.
-- The optimization must still recognize such filters as always false for not-matched rows.

SET enable_analyzer = 1;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_outer_to_inner_l;
DROP TABLE IF EXISTS t_outer_to_inner_r;

CREATE TABLE t_outer_to_inner_l (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY (k, id);
CREATE TABLE t_outer_to_inner_r (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY (k, id);

-- Left has k in [0, 15), right has k in [5, 20), so both sides have not-matched rows.
INSERT INTO t_outer_to_inner_l SELECT number, number % 15 FROM numbers(1000);
INSERT INTO t_outer_to_inner_r SELECT number, 5 + (number % 15) FROM numbers(1000);

-- Filter on both sides: FULL is converted to INNER.
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_outer_to_inner_l AS l
    FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
    WHERE l.k = 10 AND r.k = 10
) WHERE trim(explain) LIKE 'Type:%';

-- Filter on the left side only: FULL is converted to LEFT.
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_outer_to_inner_l AS l
    FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
    WHERE l.k = 10
) WHERE trim(explain) LIKE 'Type:%';

-- Filter on the right side keeps left NULLs: FULL is converted to RIGHT.
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_outer_to_inner_l AS l
    FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
    WHERE l.k IS NULL AND r.k = 16
) WHERE trim(explain) LIKE 'Type:%';

-- Filter passes NULL rows of the left side: FULL must be kept.
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_outer_to_inner_l AS l
    FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
    WHERE coalesce(l.k, 16) = 16 AND coalesce(r.k, 16) = 16
) WHERE trim(explain) LIKE 'Type:%';

-- LEFT with a filter on the right side: converted to INNER.
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_outer_to_inner_l AS l
    LEFT JOIN t_outer_to_inner_r AS r ON l.k = r.k
    WHERE r.k = 10 AND l.id >= 0
) WHERE trim(explain) LIKE 'Type:%';

-- RIGHT with a filter on the left side: converted to INNER.
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_outer_to_inner_l AS l
    RIGHT JOIN t_outer_to_inner_r AS r ON l.k = r.k
    WHERE l.k = 10 AND r.id >= 0
) WHERE trim(explain) LIKE 'Type:%';

-- Results must be the same with and without the optimization.
SELECT count(), sum(l.id), sum(r.id) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k = 10 AND r.k = 10
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(l.id), sum(r.id) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k = 10 AND r.k = 10
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

SELECT count(), sum(l.id), sum(coalesce(r.id, 0)) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k = 2
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(l.id), sum(coalesce(r.id, 0)) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k = 2
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

SELECT count(), sum(l.id), sum(r.id) FROM t_outer_to_inner_l AS l
RIGHT JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k = 10 AND r.id >= 0
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(l.id), sum(r.id) FROM t_outer_to_inner_l AS l
RIGHT JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k = 10 AND r.id >= 0
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

SELECT count(), sum(coalesce(l.id, 0)), sum(r.id) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k IS NULL AND r.k = 16
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(coalesce(l.id, 0)), sum(r.id) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE l.k IS NULL AND r.k = 16
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE coalesce(l.k, 16) = 16 AND coalesce(r.k, 16) = 16
SETTINGS query_plan_convert_outer_join_to_inner_join = 0;

SELECT count(), sum(coalesce(l.id, 0)), sum(coalesce(r.id, 0)) FROM t_outer_to_inner_l AS l
FULL JOIN t_outer_to_inner_r AS r ON l.k = r.k
WHERE coalesce(l.k, 16) = 16 AND coalesce(r.k, 16) = 16
SETTINGS query_plan_convert_outer_join_to_inner_join = 1;

DROP TABLE t_outer_to_inner_l;
DROP TABLE t_outer_to_inner_r;
