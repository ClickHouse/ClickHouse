-- `CREATE [HYPOTHETICAL] INDEX ... (<expr>) TYPE ...` could produce an AST that did not survive
-- the format-parse-format round trip, tripping the `Inconsistent AST formatting` logical error in
-- debug / sanitizer builds (issue #109163). Three independent causes:
--   1. An extra-parenthesized single expression (e.g. `((a()))`) kept a redundant top-level
--      parenthesization flag that the index's own `(...)` bracket makes meaningless.
--   2. A single expression whose canonical form itself starts with a `(` that closes before the
--      end (`(a, b).1`, `(x, y) -> x`, `(a + b) * c`) formatted without the index bracket, so the
--      re-parse swallowed that leading `(` as the bracket and dropped the trailing operator.
--   3. A scalar subquery / VALUES expression formats as `(SELECT ...)`: the leading `(` encloses
--      the whole node, so cause 2's early-close scan misses it, but the re-parse still swallows
--      that `(` as the index bracket and the bare `SELECT ...` is not a valid order-by list.
--   4. A single tuple literal formats as `(1, 2)`: a bare bracket re-parses as a multi-column
--      order-by list and is rebuilt as a `tuple(...)` function, so the string round-trips but the
--      AST does not, still tripping the tree-hash comparison the round-trip check runs first.

DROP TABLE IF EXISTS t04502;
CREATE TABLE t04502 (a UInt32, b UInt32, c String) ENGINE = MergeTree ORDER BY a;

-- Original reproducer: must not abort. `a` is not a function, so a plain UNKNOWN_FUNCTION
-- fires once the round-trip check passes cleanly.
CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a; -- { serverError UNKNOWN_FUNCTION }
CREATE INDEX i1 ON t04502 ((a())) TYPE a; -- { serverError UNKNOWN_FUNCTION }
-- Tuple-literal reproducer: must not abort. Constants are rejected in a secondary index, so a
-- plain INCORRECT_QUERY fires once the round-trip (tree-hash) check passes cleanly.
CREATE INDEX i2 ON t04502 ((1, 2)) TYPE minmax; -- { serverError INCORRECT_QUERY }

-- format(x) must equal format(format(x)) for every index expression; this is exactly what the
-- internal round-trip check verifies. Redundant-parenthesization class:
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + 1)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + 1)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 (((a))) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 (((a))) TYPE minmax'));

-- Leading-parenthesis class: expressions whose canonical form starts with a `(` that closes early.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b).1) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b).1) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b).1)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b).1)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b) -> a)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b) -> a)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + b) * a) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + b) * a) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a, b).1) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a, b).1) TYPE minmax'));

-- Subquery / VALUES class: the leading `(` encloses the whole node, so the wrapper must be kept
-- (the bare bracket cannot re-parse as an order-by list).
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT 1)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT 1)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((VALUES (1))) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((VALUES (1))) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT max(a) FROM t04502)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT max(a) FROM t04502)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((SELECT 1)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((SELECT 1)) TYPE minmax'));
-- An aliased subquery already carries its own required `(...)`, so it must not be over-wrapped.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT 1) AS s) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT 1) AS s) TYPE minmax'));

-- Tuple-literal class: a single tuple literal keeps the wrapper so the re-parse rebuilds the same
-- `ASTLiteral(Tuple)` node instead of splitting the comma into a multi-column order-by list.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((1, 2)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((1, 2)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((1, 2)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((1, 2)) TYPE minmax'));

-- Concrete formatted output: redundant parens are dropped, leading-paren forms keep the wrapper.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a + 1)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b).1) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b).1)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b) -> a)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + b) * a) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT 1)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((VALUES (1))) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((SELECT 1) AS s) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((1, 2)) TYPE minmax');

-- The fix must not over-reach: a multi-element tuple keeps its single parens (they are the value's
-- representation, not redundant grouping) and an alias keeps its required parens.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (a, b) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (tuple(a, b)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((lower(c) AS lc)) TYPE tokenbf_v1(1024, 2, 0)');

DROP TABLE t04502;
