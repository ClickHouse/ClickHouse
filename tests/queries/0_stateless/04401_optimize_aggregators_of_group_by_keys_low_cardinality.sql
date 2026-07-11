-- Tags: no-parallel-replicas
-- optimize_aggregators_of_group_by_keys must eliminate min/max/any/anyLast of a LowCardinality
-- GROUP BY key just as for a plain key, so a HAVING predicate on the key pushes down to storage
-- and uses skip indexes. See https://github.com/ClickHouse/ClickHouse/issues/110041

SET enable_analyzer = 1;
SET optimize_aggregators_of_group_by_keys = 1;
SET enable_full_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_query_condition_cache = 0;
SET optimize_trivial_count_query = 0;

DROP TABLE IF EXISTS t_lc_group_key;

CREATE TABLE t_lc_group_key
(
    id UInt64,
    s LowCardinality(String),
    INDEX s_text s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO t_lc_group_key
SELECT number, if(number % 1024 = 42, 'rare token', concat('ordinary token ', toString(number)))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

SELECT '-- the aggregate wrappers are eliminated: same single group as the direct predicate';
SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING s = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING min(s) = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING max(s) = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING any(s) = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING anyLast(s) = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING any(s = 'rare token');

SELECT '-- skip-index pruning fires for HAVING min(s) = ... (aggregate pushed down to the text index): 8/128';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING min(s) = 'rare token')
WHERE explain ILIKE '%Granules: %/128%' AND explain ILIKE '%/128%' AND explain NOT ILIKE '%128/128%';

SELECT '-- skip-index pruning fires for HAVING any(s = ...) too: 8/128';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT s, count() FROM t_lc_group_key GROUP BY s HAVING any(s = 'rare token'))
WHERE explain ILIKE '%Granules: %/128%' AND explain ILIKE '%/128%' AND explain NOT ILIKE '%128/128%';

SELECT '-- aggregate is gone from the query tree (no min/max/any/anyLast function remains)';
SELECT countIf(explain ILIKE '%function_name: min%' OR explain ILIKE '%function_name: max%'
    OR explain ILIKE '%function_name: any%' OR explain ILIKE '%function_name: anyLast%')
FROM (EXPLAIN QUERY TREE SELECT min(s) FROM t_lc_group_key GROUP BY s);

DROP TABLE t_lc_group_key;

SELECT '-- LowCardinality wrapper matrix: result must be identical with the optimization on and off';
DROP TABLE IF EXISTS t_lc_matrix;
CREATE TABLE t_lc_matrix (a LowCardinality(String), b LowCardinality(Nullable(String)), c LowCardinality(UInt8))
ENGINE = Memory
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_lc_matrix SELECT toString(number % 5), toString(number % 3), number % 7 FROM numbers(1000);

SET allow_suspicious_low_cardinality_types = 1;
SELECT min(a) AS m FROM t_lc_matrix GROUP BY a ORDER BY m;
SELECT max(b) AS m FROM t_lc_matrix GROUP BY b ORDER BY m;
SELECT any(c) AS m FROM t_lc_matrix GROUP BY c ORDER BY m;
SELECT anyLast(a) AS m FROM t_lc_matrix GROUP BY a ORDER BY m;

DROP TABLE t_lc_matrix;

SELECT '-- IN subquery must not break: the pass re-resolves ordinary functions but must skip non-column/function arguments (a non-correlated subquery QueryNode has no result type)';
SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(3));
SELECT s, count() FROM (SELECT toLowCardinality(toString(number % 5)) AS s, number AS id FROM numbers(20))
WHERE id IN (SELECT number FROM numbers(10)) GROUP BY s HAVING min(s) = '1' ORDER BY s;

-- Correlated scalar subquery whose projection is an eliminated aggregate over a LowCardinality
-- key: the rewrite flips the subquery result type String -> LowCardinality(String), so both the
-- query node's projection_columns metadata and any parent operator on the subquery must be
-- refreshed. Otherwise the outer `= 'm'` still expects the old type and the query tree validator
-- (or PlannerCorrelatedSubqueries) throws. Result must be identical with the optimization on/off.
SET allow_experimental_correlated_subqueries = 1, allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_lc_correlated;
CREATE TABLE t_lc_correlated (id UInt64, s LowCardinality(String), sn LowCardinality(Nullable(String)), u LowCardinality(UInt8))
ENGINE = Memory;
INSERT INTO t_lc_correlated VALUES (1, 'x', 'x', 1), (2, 'm', 'm', 7), (3, 'z', NULL, 3);

SELECT '-- correlated subquery in WHERE over LowCardinality key (min/max/any/anyLast), opt on == off';
SELECT id FROM t_lc_correlated AS o WHERE (SELECT min(s) FROM t_lc_correlated AS i WHERE i.id = o.id GROUP BY s) = 'm' ORDER BY id;
SELECT id FROM t_lc_correlated AS o WHERE (SELECT max(sn) FROM t_lc_correlated AS i WHERE i.id = o.id GROUP BY sn) = 'm' ORDER BY id;
SELECT id FROM t_lc_correlated AS o WHERE (SELECT any(u) FROM t_lc_correlated AS i WHERE i.id = o.id GROUP BY u) = 7 ORDER BY id;
SELECT id FROM t_lc_correlated AS o WHERE (SELECT anyLast(s) FROM t_lc_correlated AS i WHERE i.id = o.id GROUP BY s) = 'm' ORDER BY id;

SELECT '-- correlated subquery in SELECT position over LowCardinality key';
SELECT id, (SELECT min(s) FROM t_lc_correlated AS i WHERE i.id = o.id GROUP BY s) AS m FROM t_lc_correlated AS o ORDER BY id;

DROP TABLE t_lc_correlated;
