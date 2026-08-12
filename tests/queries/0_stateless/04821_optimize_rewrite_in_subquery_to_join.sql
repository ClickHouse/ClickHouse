SET enable_analyzer = 1;
SET optimize_rewrite_in_subquery_to_join = 1;
-- Pinned so that CI setting randomization does not change the EXPLAIN output or disable the rewrite
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';
SET transform_null_in = 0;
SET max_rows_in_set = 0, max_bytes_in_set = 0;

DROP TABLE IF EXISTS t_outer;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_set;

CREATE TABLE t_outer (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_set (y UInt64) ENGINE = Set;

INSERT INTO t_outer VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO t_right VALUES (1, 10), (2, 10), (3, 30), (4, 50);
INSERT INTO t_set VALUES (10);

SELECT '-- IN rewritten to LEFT SEMI JOIN';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- NOT IN rewritten to LEFT ANTI JOIN';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val NOT IN (SELECT y FROM t_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- tuple IN rewritten with two key equalities';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE (val, val + 1) IN (SELECT y, x FROM t_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- IN over a primary key column is not rewritten (index analysis is better)';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE id IN (SELECT y FROM t_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- NOT IN over a primary key column is rewritten (negative predicates do not prune)';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE id NOT IN (SELECT y FROM t_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- transform_null_in = 1 is not rewritten';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right) SETTINGS transform_null_in = 1
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- IN under OR is not rewritten';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right) OR val = 0
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- IN in the SELECT list is not rewritten';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT val IN (SELECT y FROM t_right) FROM t_outer
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- constant left argument is not rewritten';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE 10 IN (SELECT y FROM t_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- Set-engine table on the right is not rewritten';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val IN t_set
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- explicit set size limits disable the rewrite';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right)
    SETTINGS max_rows_in_set = 1000, set_overflow_mode = 'break'
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- full_sorting_merge cannot run SEMI/ANTI joins, no rewrite';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right) SETTINGS join_algorithm = 'full_sorting_merge'
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';

SELECT '-- results: IN and NOT IN with duplicates on the right';
SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right) SETTINGS optimize_rewrite_in_subquery_to_join = 0;
SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right) SETTINGS optimize_rewrite_in_subquery_to_join = 1;
SELECT count() FROM t_outer WHERE val NOT IN (SELECT y FROM t_right) SETTINGS optimize_rewrite_in_subquery_to_join = 0;
SELECT count() FROM t_outer WHERE val NOT IN (SELECT y FROM t_right) SETTINGS optimize_rewrite_in_subquery_to_join = 1;

SELECT '-- results: empty subquery';
SELECT count() FROM t_outer WHERE val IN (SELECT y FROM t_right WHERE 0);
SELECT count() FROM t_outer WHERE val NOT IN (SELECT y FROM t_right WHERE 0);

SELECT '-- results: NULLs on both sides (NULL is not in, NULL is not not-in the set)';
DROP TABLE IF EXISTS t_nulls_left;
DROP TABLE IF EXISTS t_nulls_right;
CREATE TABLE t_nulls_left (k Nullable(UInt64), tag String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_nulls_right (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nulls_left VALUES (1, 'one'), (NULL, 'null'), (3, 'three');
INSERT INTO t_nulls_right VALUES (1), (NULL);
SELECT '-- IN with a Nullable left key is rewritten (both paths drop NULL-keyed rows)';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_nulls_left WHERE k IN (SELECT k FROM t_nulls_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';
SELECT '-- NOT IN with a Nullable left key is not rewritten (notIn drops NULL-keyed rows, ANTI JOIN keeps them)';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_nulls_left WHERE k NOT IN (SELECT k FROM t_nulls_right)
) WHERE trimLeft(explain) LIKE 'Type: %' OR trimLeft(explain) LIKE 'Strictness: %';
SELECT tag FROM t_nulls_left WHERE k IN (SELECT k FROM t_nulls_right) ORDER BY tag SETTINGS optimize_rewrite_in_subquery_to_join = 0;
SELECT tag FROM t_nulls_left WHERE k IN (SELECT k FROM t_nulls_right) ORDER BY tag SETTINGS optimize_rewrite_in_subquery_to_join = 1;
SELECT tag FROM t_nulls_left WHERE k NOT IN (SELECT k FROM t_nulls_right) ORDER BY tag SETTINGS optimize_rewrite_in_subquery_to_join = 0;
SELECT tag FROM t_nulls_left WHERE k NOT IN (SELECT k FROM t_nulls_right) ORDER BY tag SETTINGS optimize_rewrite_in_subquery_to_join = 1;

SELECT '-- results: type widening keeps out-of-range values not-in';
SELECT toInt64(-1) IN (SELECT toUInt8(255) FROM system.one) SETTINGS optimize_rewrite_in_subquery_to_join = 0;
SELECT toInt64(-1) NOT IN (SELECT toUInt8(255) FROM system.one) SETTINGS optimize_rewrite_in_subquery_to_join = 0;
SELECT count() FROM (SELECT toInt64(-1) AS v FROM system.one) WHERE v IN (SELECT toUInt8(255) FROM system.one) SETTINGS optimize_rewrite_in_subquery_to_join = 1;
SELECT count() FROM (SELECT toInt64(-1) AS v FROM system.one) WHERE v NOT IN (SELECT toUInt8(255) FROM system.one) SETTINGS optimize_rewrite_in_subquery_to_join = 1;

SELECT '-- results: LowCardinality keys';
DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc VALUES ('a'), ('b'), ('c');
SELECT count() FROM t_lc WHERE s IN (SELECT 'b');
SELECT count() FROM t_lc WHERE s NOT IN (SELECT 'b');

SELECT '-- results: name collision between inner and outer columns';
SELECT count() FROM t_outer WHERE val IN (SELECT val * 10 AS val FROM t_outer WHERE id = 1);

DROP TABLE t_outer;
DROP TABLE t_right;
DROP TABLE t_set;
DROP TABLE t_nulls_left;
DROP TABLE t_nulls_right;
DROP TABLE t_lc;
