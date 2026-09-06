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

SELECT '-- QUALIFY is a filter section like HAVING (own clause plumbing, runs after window processing):';
SELECT '-- a type-insensitive predicate on the key must push down to the skip index just like HAVING';
-- Regression for https://github.com/ClickHouse/ClickHouse/pull/110059: the pass must track the
-- QUALIFY root as a filter root exactly like HAVING/WHERE/PREWHERE.
SELECT s, count() FROM t_lc_group_key GROUP BY s QUALIFY s = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s QUALIFY min(s) = 'rare token';
SELECT s, count() FROM t_lc_group_key GROUP BY s QUALIFY anyLast(s) = 'rare token';
SELECT '-- skip-index pruning fires for QUALIFY min(s) = ... just like HAVING: 8/128';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT s, count() FROM t_lc_group_key GROUP BY s QUALIFY min(s) = 'rare token')
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

SELECT '-- type-sensitive parent inside HAVING must observe the analyzed type, not the key type';
-- A filter section is a predicate position only for functions transparent to LowCardinality
-- (comparisons run on the nested column, so leaving the bare key preserves values and pushdown). A
-- function that observes LowCardinality (e.g. toTypeName) would otherwise see LowCardinality(String)
-- and change its result inside HAVING, dropping rows. Regression for
-- https://github.com/ClickHouse/ClickHouse/pull/110059: toTypeName(min(s)) = 'String' in HAVING must
-- stay true (result identical with the optimization on and off).
DROP TABLE IF EXISTS t_lc_having_type;
CREATE TABLE t_lc_having_type (s LowCardinality(String), sn LowCardinality(Nullable(String)), u LowCardinality(UInt8))
ENGINE = Memory
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_lc_having_type VALUES ('x', 'x', 1), ('y', NULL, 2);
SET allow_suspicious_low_cardinality_types = 1;
SELECT count() FROM t_lc_having_type GROUP BY s HAVING toTypeName(min(s)) = 'String';
SELECT count() FROM t_lc_having_type GROUP BY s
    HAVING toTypeName(max(s)) = 'String' AND toTypeName(any(s)) = 'String'
        AND toTypeName(anyLast(s)) = 'String' AND toTypeName(anyHeavy(s)) = 'String';
SELECT count() FROM t_lc_having_type GROUP BY sn HAVING toTypeName(min(sn)) = 'Nullable(String)';
SELECT count() FROM t_lc_having_type GROUP BY u HAVING toTypeName(min(u)) = 'UInt8';
SELECT '-- type-insensitive predicate in the same HAVING still pushes down (equality on the key)';
SELECT s, count() FROM t_lc_having_type GROUP BY s HAVING min(s) = 'x' AND toTypeName(min(s)) = 'String' ORDER BY s;
DROP TABLE t_lc_having_type;

SELECT '-- UNION subquery argument of a parent function must not be dereferenced by the pass';
-- A UNION subquery has no getResultType() (unlike a correlated QueryNode) and throws
-- UNSUPPORTED_METHOD if dereferenced. reresolveIfArgumentTypesChanged must never call it on a UNION
-- argument. Regression for https://github.com/ClickHouse/ClickHouse/pull/110059: these queries have
-- a UNION as a function/IN argument and must run (no UNSUPPORTED_METHOD), with results identical to
-- the optimization off.
DROP TABLE IF EXISTS t_lc_union;
CREATE TABLE t_lc_union (id UInt32, s LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_lc_union VALUES (1, 'x'), (2, 'y');
SELECT id FROM t_lc_union WHERE s IN (SELECT min(s) FROM t_lc_union GROUP BY s UNION ALL SELECT 'z') ORDER BY id;
SELECT id FROM t_lc_union WHERE s = (SELECT min(s) FROM t_lc_union GROUP BY s LIMIT 1) ORDER BY id;
SELECT count() FROM t_lc_union WHERE s IN (SELECT max(s) FROM t_lc_union GROUP BY s UNION ALL SELECT anyLast(s) FROM t_lc_union GROUP BY s);
DROP TABLE t_lc_union;

SELECT '-- type-sensitive QUALIFY toTypeName(min(key)) must observe the analyzed type across the LC matrix';
DROP TABLE IF EXISTS t_lc_qualify_type;
CREATE TABLE t_lc_qualify_type (s LowCardinality(String), sn LowCardinality(Nullable(String)), u LowCardinality(UInt8))
ENGINE = Memory
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_lc_qualify_type VALUES ('x', 'x', 1), ('y', NULL, 2);
SET allow_suspicious_low_cardinality_types = 1;
SELECT count() FROM t_lc_qualify_type GROUP BY s QUALIFY toTypeName(min(s)) = 'String';
SELECT count() FROM t_lc_qualify_type GROUP BY s
    QUALIFY toTypeName(max(s)) = 'String' AND toTypeName(any(s)) = 'String'
        AND toTypeName(anyLast(s)) = 'String' AND toTypeName(anyHeavy(s)) = 'String';
SELECT count() FROM t_lc_qualify_type GROUP BY sn QUALIFY toTypeName(min(sn)) = 'Nullable(String)';
SELECT count() FROM t_lc_qualify_type GROUP BY u QUALIFY toTypeName(min(u)) = 'UInt8';
SELECT '-- type-insensitive and type-sensitive parents together in the same QUALIFY';
SELECT s, count() FROM t_lc_qualify_type GROUP BY s QUALIFY min(s) = 'x' AND toTypeName(min(s)) = 'String' ORDER BY s;
DROP TABLE t_lc_qualify_type;

SELECT '-- correlated scalar subquery as the whole filter root must not underflow the filter depth';
-- enterImpl increments filter_depth only for non-QUERY, non-LAMBDA nodes; a correlated scalar
-- subquery that is itself the whole HAVING/QUALIFY root saves and resets the filter context instead.
-- leaveImpl must therefore not decrement filter_depth for such a node, or it underflows to size_t(-1)
-- and the next filter root wraps back to 0 and loses the bare-key rewrite (and its pushdown) for a
-- later LC aggregate elimination. Regression for
-- https://github.com/ClickHouse/ClickHouse/pull/110059: the later QUALIFY min(s) must still be a
-- type-insensitive predicate on the bare key (result and pushdown-eligibility identical on and off).
DROP TABLE IF EXISTS t_lc_filter_root;
DROP TABLE IF EXISTS u_lc_filter_root;
CREATE TABLE t_lc_filter_root
    (id UInt64, s LowCardinality(String), sn LowCardinality(Nullable(String)), u LowCardinality(UInt8))
ENGINE = Memory
SETTINGS allow_suspicious_low_cardinality_types = 1;
CREATE TABLE u_lc_filter_root (id UInt64) ENGINE = Memory;
INSERT INTO t_lc_filter_root VALUES (1, 'x', 'x', 1), (2, 'y', NULL, 2);
INSERT INTO u_lc_filter_root VALUES (1), (2);
SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_correlated_subqueries = 1;
SELECT '-- correctness across the LC matrix (correlated subquery is the whole HAVING, later QUALIFY min(key))';
SELECT s FROM t_lc_filter_root AS o GROUP BY s, o.id
    HAVING (SELECT any(u_lc_filter_root.id = o.id) FROM u_lc_filter_root) QUALIFY min(o.s) = 'x' ORDER BY s;
SELECT sn FROM t_lc_filter_root AS o GROUP BY sn, o.id
    HAVING (SELECT any(u_lc_filter_root.id = o.id) FROM u_lc_filter_root) QUALIFY min(o.sn) = 'x' ORDER BY sn;
SELECT u FROM t_lc_filter_root AS o GROUP BY u, o.id
    HAVING (SELECT any(u_lc_filter_root.id = o.id) FROM u_lc_filter_root) QUALIFY min(o.u) = 1 ORDER BY u;
SELECT '-- the later QUALIFY min(s) keeps the bare LowCardinality key (0 = no output cast = pushdown-eligible)';
SELECT countIf(explain LIKE '%_CAST%' AND explain LIKE '%String%') FROM (
    EXPLAIN QUERY TREE
    SELECT count() OVER () FROM t_lc_filter_root AS o GROUP BY s, o.id
        HAVING (SELECT any(u_lc_filter_root.id = o.id) FROM u_lc_filter_root) QUALIFY min(o.s) = 'x');
SELECT '-- correlated subquery is the whole HAVING, later ORDER BY min(s) still eliminated (observable type String)';
SELECT DISTINCT toTypeName(min(o.s)) FROM t_lc_filter_root AS o GROUP BY s, o.id
    HAVING (SELECT any(u_lc_filter_root.id = o.id) FROM u_lc_filter_root) ORDER BY min(o.s);
DROP TABLE t_lc_filter_root;
DROP TABLE u_lc_filter_root;
