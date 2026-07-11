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

SELECT '-- observable result type is preserved: min/max/any/anyLast of a LowCardinality key still';
SELECT '-- report the aggregate result type (String), not the key type LowCardinality(String)';
SELECT DISTINCT toTypeName(min(s)) FROM t_lc_group_key GROUP BY s;
SELECT DISTINCT toTypeName(max(s)) FROM t_lc_group_key GROUP BY s;
SELECT DISTINCT toTypeName(any(s)) FROM t_lc_group_key GROUP BY s;
SELECT DISTINCT toTypeName(anyLast(s)) FROM t_lc_group_key GROUP BY s;
SELECT '-- and it is preserved when the same aggregate is also used in a HAVING predicate (mixed)';
SELECT DISTINCT toTypeName(min(s)) FROM t_lc_group_key GROUP BY s HAVING min(s) = 'rare token';
SELECT '-- the result column schema of a subquery is unchanged (String), so type-sensitive sinks work';
SELECT DISTINCT toTypeName(m) FROM (SELECT min(s) AS m FROM t_lc_group_key GROUP BY s);
SELECT '-- pruning is still preserved for that mixed projection+HAVING query: 8/128';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT min(s) AS m, count() FROM t_lc_group_key GROUP BY s HAVING min(s) = 'rare token')
WHERE explain ILIKE '%Granules: %/128%' AND explain ILIKE '%/128%' AND explain NOT ILIKE '%128/128%';

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
-- key. The subquery projection is an output position, so the key is cast back to the aggregate's
-- analyzed type; the subquery result type stays String and the outer `= 'm'` predicate is
-- unchanged (otherwise PlannerCorrelatedSubqueries / the query tree validator would throw on the
-- header mismatch). Result must be identical with the optimization on/off.
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

-- The pass eliminates every value-preserving single-argument aggregate it handles, not just
-- min/max/any/anyLast: anyHeavy and the RESPECT NULLS variants any_respect_nulls /
-- anyLast_respect_nulls also return an actual element of the group, so over a GROUP BY key they
-- equal the key and are dropped. Aliases (first_value/last_value/any_value, *RespectNulls) resolve
-- to these canonical names before the pass runs. singleValueOrNull is intentionally excluded (it
-- returns NULL unless the group has a single distinct value, so it is not value-preserving).
SELECT '-- anyHeavy / RESPECT NULLS variants of a LowCardinality key are eliminated too';
DROP TABLE IF EXISTS t_lc_more_aggs;
CREATE TABLE t_lc_more_aggs
(
    id UInt64,
    s LowCardinality(String),
    INDEX s_text s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64;
INSERT INTO t_lc_more_aggs
SELECT number, if(number % 1024 = 42, 'rare token', concat('ordinary token ', toString(number)))
FROM numbers(8192)
SETTINGS max_insert_threads = 1;

SELECT '-- same single group as the direct predicate (aggregate wrapper eliminated)';
SELECT s, count() FROM t_lc_more_aggs GROUP BY s HAVING anyHeavy(s) = 'rare token';
SELECT s, count() FROM t_lc_more_aggs GROUP BY s HAVING any(s) RESPECT NULLS = 'rare token';
SELECT s, count() FROM t_lc_more_aggs GROUP BY s HAVING anyLast(s) RESPECT NULLS = 'rare token';

SELECT '-- observable result type preserved (String, not LowCardinality(String))';
SELECT DISTINCT toTypeName(anyHeavy(s)) FROM t_lc_more_aggs GROUP BY s;
SELECT DISTINCT toTypeName(any(s) RESPECT NULLS) FROM t_lc_more_aggs GROUP BY s;
SELECT DISTINCT toTypeName(anyLast(s) RESPECT NULLS) FROM t_lc_more_aggs GROUP BY s;

SELECT '-- aggregate gone from the query tree for anyHeavy';
SELECT countIf(explain ILIKE '%function_type: aggregate%')
FROM (EXPLAIN QUERY TREE SELECT anyHeavy(s) FROM t_lc_more_aggs GROUP BY s);

SELECT '-- skip-index pruning fires for HAVING anyHeavy(s) = ... just like min(s): 8/128';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT s, count() FROM t_lc_more_aggs GROUP BY s HAVING anyHeavy(s) = 'rare token')
WHERE explain ILIKE '%Granules: %/128%' AND explain ILIKE '%/128%' AND explain NOT ILIKE '%128/128%';

DROP TABLE t_lc_more_aggs;

SELECT '-- wrapper matrix for the newly-covered aggregates: result identical with optimization on and off';
DROP TABLE IF EXISTS t_lc_more_matrix;
CREATE TABLE t_lc_more_matrix (a LowCardinality(String), b LowCardinality(Nullable(String)), c LowCardinality(UInt8))
ENGINE = Memory
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_lc_more_matrix SELECT toString(number % 5), toString(number % 3), number % 7 FROM numbers(1000);
SET allow_suspicious_low_cardinality_types = 1;
SELECT anyHeavy(a) AS m FROM t_lc_more_matrix GROUP BY a ORDER BY m;
SELECT any(b) RESPECT NULLS AS m FROM t_lc_more_matrix GROUP BY b ORDER BY m;
SELECT anyLast(c) RESPECT NULLS AS m FROM t_lc_more_matrix GROUP BY c ORDER BY m;
DROP TABLE t_lc_more_matrix;

SELECT '-- eliminated aggregate inside a lambda body: LambdaNode result type stays the analyzed type';
-- The lambda body min(s) is an output-ish position carried by the higher-order function signature,
-- not a storage-pushdown predicate, so the eliminated LowCardinality key is cast back to String.
-- Regression for https://github.com/ClickHouse/ClickHouse/pull/110059 (visitLambda type mismatch).
DROP TABLE IF EXISTS t_lc_lambda;
CREATE TABLE t_lc_lambda (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_lc_lambda SELECT toString(number % 3) FROM numbers(30);
SELECT count() FROM t_lc_lambda GROUP BY s HAVING arrayMap(x -> min(s), [1]) = [s] ORDER BY min(s);
SELECT arrayMap(x -> max(s), [1]) AS m FROM t_lc_lambda GROUP BY s ORDER BY m;
SELECT '-- observable lambda-body type preserved (Array(String), not Array(LowCardinality(String)))';
SELECT DISTINCT toTypeName(arrayMap(x -> any(s), [1])) FROM t_lc_lambda GROUP BY s;
SELECT DISTINCT toTypeName(arrayMap(x -> anyLast(s), [1])) FROM t_lc_lambda GROUP BY s;
SELECT '-- nested lambdas over the eliminated key';
SELECT count() FROM t_lc_lambda GROUP BY s HAVING arrayMap(y -> arrayMap(x -> min(s), [1])[1], [1]) = [s];
DROP TABLE t_lc_lambda;
