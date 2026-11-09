-- Tags: no-parallel-replicas

-- LogicalExpressionOptimizer rewrites `x = c1 OR x = c2 OR ...` into `x IN (c1, c2, ...)`
-- (and the notEquals/has counterparts). For a Variant expression with
-- use_variant_default_implementation_for_comparisons = 0, equals resolves to UInt8 but the
-- resulting `in` resolves through the variant adaptor to Nullable(UInt8). The rewrite must not
-- change the node's result type (ancestors already captured it) - otherwise a debug/sanitizer
-- build raises a LOGICAL_ERROR from the query-tree ValidationChecker. Reproducer shape from AST fuzzer STID 0250-20a8.

SET enable_analyzer = 1;
SET use_variant_default_implementation_for_comparisons = 0;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;

DROP TABLE IF EXISTS t_var;
CREATE TABLE t_var (id UInt8, b Variant(UInt256), x UInt8) ENGINE = Memory;
INSERT INTO t_var VALUES (1, 1, 1), (2, 10, 0), (3, 22, 1), (4, 6, 1);

-- OR -> IN: scalar parent (previously raised the logical error).
SELECT id, NOT ((b = '10') OR (b = '22') OR (b = '6')) FROM t_var ORDER BY id;

-- OR -> IN: aggregate (windowFunnel + -Null combinator) parent, mirroring the fuzzer query.
SELECT windowFunnel(1)(toUInt8(1), (b = '10') OR (b = '22') OR (b = '6')) FROM t_var GROUP BY x ORDER BY x;

-- notEquals AND-chain -> NOT IN: needs a second surviving operand so the AND is not collapsed to one node.
SELECT id, NOT ((b != '10') AND (b != '22') AND (b != '6') AND (x = 1)) FROM t_var ORDER BY id;
SELECT windowFunnel(1)(toUInt8(1), (b != '10') AND (b != '22') AND (b != '6') AND (x = 1)) FROM t_var GROUP BY x ORDER BY x;

-- has(const_array, variant_column) -> in: constant Array(Variant) needle column.
SELECT id, NOT has([1::UInt256::Variant(UInt256), 2::UInt256::Variant(UInt256)], b) FROM t_var ORDER BY id
SETTINGS optimize_rewrite_has_to_in = 1;

-- Dynamic OR-chain: `in` rejects Dynamic outright, so the rewrite must be skipped (previously threw).
SELECT (d = '1') OR (d = '2') OR (d = '3') FROM (SELECT materialize('1')::Dynamic AS d);

-- Same for `notIn`, and for the wider set of types that merely *contain* a dynamic structure
-- (Array(Dynamic), JSON): those get no nullability adaptor, so the comparisons stay UInt8 and only a
-- check made before the function is built can keep the chain.
SELECT ad, x FROM (SELECT ['1']::Array(Dynamic) AS ad, materialize(toUInt8(1)) AS x)
WHERE (ad != ['2']::Array(Dynamic)) AND (ad != ['3']::Array(Dynamic)) AND (ad != ['4']::Array(Dynamic)) AND (x = 1);
SELECT (ad = ['2']::Array(Dynamic)) OR (ad = ['3']::Array(Dynamic)) OR (ad = ['4']::Array(Dynamic))
FROM (SELECT ['1']::Array(Dynamic) AS ad);
SELECT j, x FROM (SELECT '{"a":1}'::JSON AS j, materialize(toUInt8(1)) AS x)
WHERE (j != '{"a":2}'::JSON) AND (j != '{"a":3}'::JSON) AND (j != '{"a":4}'::JSON) AND (x = 1);
SELECT (j = '{"a":2}'::JSON) OR (j = '{"a":3}'::JSON) OR (j = '{"a":4}'::JSON) FROM (SELECT '{"a":1}'::JSON AS j);

-- Optimization-preservation: the guard must skip only the Variant-under-adaptor case, and keep
-- firing for every type where `in`/`notIn` preserves the result type.
-- OR -> IN skipped for Variant only while equals is not nullable (setting = 0):
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (b = '1') OR (b = '2') OR (b = '3') FROM t_var) WHERE explain ILIKE '%function_name: in%';
-- ... and still fires for the same Variant column once equals is nullable too (setting = 1), so the
-- guard must key on the drift and not on the type:
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (b = '1') OR (b = '2') OR (b = '3') FROM t_var
    SETTINGS use_variant_default_implementation_for_comparisons = 1) WHERE explain ILIKE '%function_name: in%';
-- ... but still fires for plain String:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (materialize('1') = '1') OR (materialize('1') = '2') OR (materialize('1') = '3')) WHERE explain ILIKE '%function_name: in%';
-- ... and still fires for a Nullable column:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (n = 1) OR (n = 2) OR (n = 3) FROM (SELECT materialize(CAST(1, 'Nullable(UInt8)')) AS n)) WHERE explain ILIKE '%function_name: in%';
-- ... and still fires for a LowCardinality column:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (l = '1') OR (l = '2') OR (l = '3') FROM (SELECT materialize(CAST('1', 'LowCardinality(String)')) AS l)) WHERE explain ILIKE '%function_name: in%';
-- notEquals -> NOT IN skipped for Variant (setting = 0). With the setting ON, notEquals is nullable,
-- so the AND is nullable and the caller returns before any conversion - `notIn` never fires for that
-- configuration regardless of this change, so only the setting = 0 state is asserted here:
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (b != '1') AND (b != '2') AND (b != '3') AND (x = 1) FROM t_var) WHERE explain ILIKE '%function_name: notIn%';
-- ... but still fires for plain String:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (s != '1') AND (s != '2') AND (s != '3') AND (y = 1) FROM (SELECT materialize('0') AS s, materialize(toUInt8(1)) AS y)) WHERE explain ILIKE '%function_name: notIn%';
-- has() -> in skipped for a Variant needle. Asserted on the plan, not only on the result: the result
-- above is protected by the ValidationChecker only in debug and sanitizer builds.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT has([1::UInt256::Variant(UInt256), 2::UInt256::Variant(UInt256)], b) FROM t_var
    SETTINGS optimize_rewrite_has_to_in = 1) WHERE explain ILIKE '%function_name: in%';
-- ... but still fires for a plain UInt8 needle:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT has([1, 2], x) FROM t_var
    SETTINGS optimize_rewrite_has_to_in = 1) WHERE explain ILIKE '%function_name: in%';

-- The NOT IN tuple must keep the resolved constant types. Building it from the `Field` values alone
-- re-derives them and collapses DateTime to UInt32, so the rewritten predicate compares a date
-- against raw seconds and matches no set element at all. Each pair must agree with the un-rewritten form.
DROP TABLE IF EXISTS t_dt;
CREATE TABLE t_dt (d Date, dt DateTime('UTC')) ENGINE = Memory;
INSERT INTO t_dt VALUES ('2020-01-01', '2020-01-01 00:00:00'), ('2020-01-02', '2020-01-02 00:00:00'), ('2020-01-03', '2020-01-03 00:00:00'), ('2020-01-04', '2020-01-04 00:00:00');

SELECT count() FROM t_dt WHERE (d != parseDateTimeBestEffort('2020-01-01')) AND (d != parseDateTimeBestEffort('2020-01-02')) AND (d != parseDateTimeBestEffort('2020-01-03'));
SELECT count() FROM t_dt WHERE (d != parseDateTimeBestEffort('2020-01-01')) AND (d != parseDateTimeBestEffort('2020-01-02')) AND (d != parseDateTimeBestEffort('2020-01-03'))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;
SELECT count() FROM t_dt WHERE (dt != toDate('2020-01-01')) AND (dt != toDate('2020-01-02')) AND (dt != toDate('2020-01-03'));
SELECT count() FROM t_dt WHERE (dt != toDate('2020-01-01')) AND (dt != toDate('2020-01-02')) AND (dt != toDate('2020-01-03'))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;
-- The rewrite must still happen for these - the point is that it now preserves the types.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_dt WHERE (d != parseDateTimeBestEffort('2020-01-01')) AND (d != parseDateTimeBestEffort('2020-01-02')) AND (d != parseDateTimeBestEffort('2020-01-03'))) WHERE explain ILIKE '%Tuple(DateTime, DateTime, DateTime)%';

-- Preserving the types is not enough on its own: the set converts every element to the expression's
-- type, while the comparison it replaces is evaluated in the wider type. For a non-midnight DateTime
-- constant against a Date expression that conversion is lossy (the row promotes to midnight and does
-- differ from the constant, but the constant truncates onto the row), so the chain must be kept.
SELECT count() FROM t_dt WHERE (d != toDateTime('2020-01-01 12:00:00', 'UTC')) AND (d != toDateTime('2020-01-02 12:00:00', 'UTC')) AND (d != toDateTime('2020-01-03 12:00:00', 'UTC'));
SELECT count() FROM t_dt WHERE (d != toDateTime('2020-01-01 12:00:00', 'UTC')) AND (d != toDateTime('2020-01-02 12:00:00', 'UTC')) AND (d != toDateTime('2020-01-03 12:00:00', 'UTC'))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;
-- ... and the rewrite is the thing being skipped there:
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_dt WHERE (d != toDateTime('2020-01-01 12:00:00', 'UTC')) AND (d != toDateTime('2020-01-02 12:00:00', 'UTC')) AND (d != toDateTime('2020-01-03 12:00:00', 'UTC'))) WHERE explain ILIKE '%function_name: notIn%';

-- The OR direction needs the same conversion check: it kept the constant types but not the
-- requirement that they convert losslessly.
SELECT count() FROM t_dt WHERE (d = toDateTime('2020-01-01 12:00:00', 'UTC')) OR (d = toDateTime('2020-01-02 12:00:00', 'UTC')) OR (d = toDateTime('2020-01-03 12:00:00', 'UTC'));
SELECT count() FROM t_dt WHERE (d = toDateTime('2020-01-01 12:00:00', 'UTC')) OR (d = toDateTime('2020-01-02 12:00:00', 'UTC')) OR (d = toDateTime('2020-01-03 12:00:00', 'UTC'))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_dt WHERE (d = toDateTime('2020-01-01 12:00:00', 'UTC')) OR (d = toDateTime('2020-01-02 12:00:00', 'UTC')) OR (d = toDateTime('2020-01-03 12:00:00', 'UTC'))) WHERE explain ILIKE '%function_name: in%';
-- ... while a midnight constant still converts losslessly, so that rewrite keeps firing:
SELECT count() FROM t_dt WHERE (d = parseDateTimeBestEffort('2020-01-01')) OR (d = parseDateTimeBestEffort('2020-01-02')) OR (d = parseDateTimeBestEffort('2020-01-03'));
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_dt WHERE (d = parseDateTimeBestEffort('2020-01-01')) OR (d = parseDateTimeBestEffort('2020-01-02')) OR (d = parseDateTimeBestEffort('2020-01-03'))) WHERE explain ILIKE '%function_name: in%';
-- ... and `k = 1 OR k = NULL` still folds and stays nullable (a NULL constant never reaches the check):
SELECT materialize(1) = 1 OR materialize(1) = NULL SETTINGS optimize_min_equality_disjunction_chain_length = 2;

DROP TABLE t_dt;
DROP TABLE t_var;
