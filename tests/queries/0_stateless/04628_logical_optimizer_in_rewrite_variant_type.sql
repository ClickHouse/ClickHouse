-- Tags: no-parallel-replicas

-- LogicalExpressionOptimizer rewrites `x = c1 OR x = c2 OR ...` into `x IN (c1, c2, ...)`
-- (and the notEquals/has counterparts). For a Variant expression with
-- use_variant_default_implementation_for_comparisons = 0, equals resolves to UInt8 but the
-- resulting `in` resolves through the variant adaptor to Nullable(UInt8). The rewrite must not
-- change the node's result type (ancestors already captured it) - otherwise a debug/sanitizer
-- build aborts in the query-tree ValidationChecker. Reproducer shape from AST fuzzer STID 0250-20a8.

SET enable_analyzer = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET use_variant_default_implementation_for_comparisons = 0;
SET optimize_min_equality_disjunction_chain_length = 3;
SET optimize_min_inequality_conjunction_chain_length = 3;

DROP TABLE IF EXISTS t_var;
CREATE TABLE t_var (id UInt8, b Variant(UInt256), x UInt8) ENGINE = Memory;
INSERT INTO t_var VALUES (1, 1, 1), (2, 10, 0), (3, 22, 1), (4, 6, 1);

-- OR -> IN: scalar parent (previously aborted).
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

-- Optimization-preservation: the guard must skip only the Variant-under-adaptor case, and keep
-- firing for every type where `in`/`notIn` preserves the result type.
-- OR -> IN skipped for Variant (setting = 0):
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (b = '1') OR (b = '2') OR (b = '3') FROM t_var) WHERE explain ILIKE '%function_name: in%';
-- ... but still fires for plain String:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (materialize('1') = '1') OR (materialize('1') = '2') OR (materialize('1') = '3')) WHERE explain ILIKE '%function_name: in%';
-- ... and still fires for a Nullable column:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (n = 1) OR (n = 2) OR (n = 3) FROM (SELECT materialize(CAST(1, 'Nullable(UInt8)')) AS n)) WHERE explain ILIKE '%function_name: in%';
-- ... and still fires for a LowCardinality column:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (l = '1') OR (l = '2') OR (l = '3') FROM (SELECT materialize(CAST('1', 'LowCardinality(String)')) AS l)) WHERE explain ILIKE '%function_name: in%';
-- notEquals -> NOT IN skipped for Variant (setting = 0):
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (b != '1') AND (b != '2') AND (b != '3') AND (x = 1) FROM t_var) WHERE explain ILIKE '%function_name: notIn%';
-- ... but still fires for plain String:
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT (s != '1') AND (s != '2') AND (s != '3') AND (y = 1) FROM (SELECT materialize('0') AS s, materialize(toUInt8(1)) AS y)) WHERE explain ILIKE '%function_name: notIn%';

DROP TABLE t_var;
