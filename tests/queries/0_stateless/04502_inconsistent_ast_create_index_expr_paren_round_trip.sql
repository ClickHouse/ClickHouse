-- `CREATE [HYPOTHETICAL] INDEX ... (<expr>) TYPE ...` could produce an AST that did not survive
-- the format-parse-format round trip, tripping the `Inconsistent AST formatting` logical error in
-- debug / sanitizer builds (issue #109163). Two independent causes:
--   1. An extra-parenthesized single expression (e.g. `((a()))`) kept a redundant top-level
--      parenthesization flag that the index's own `(...)` bracket makes meaningless.
--   2. A single expression whose canonical form itself starts with a `(` that closes before the
--      end (`(a, b).1`, `(x, y) -> x`, `(a + b) * c`) formatted without the index bracket, so the
--      re-parse swallowed that leading `(` as the bracket and dropped the trailing operator.

DROP TABLE IF EXISTS t04502;
CREATE TABLE t04502 (a UInt32, b UInt32, c String) ENGINE = MergeTree ORDER BY a;

-- Original reproducer: must not abort. `a` is not a function, so a plain UNKNOWN_FUNCTION
-- fires once the round-trip check passes cleanly.
CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a; -- { serverError UNKNOWN_FUNCTION }
CREATE INDEX i1 ON t04502 ((a())) TYPE a; -- { serverError UNKNOWN_FUNCTION }

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

-- Concrete formatted output: redundant parens are dropped, leading-paren forms keep the wrapper.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a + 1)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b).1) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b).1)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a, b) -> a)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + b) * a) TYPE minmax');

-- The fix must not over-reach: a multi-element tuple keeps its single parens (they are the value's
-- representation, not redundant grouping) and an alias keeps its required parens.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (a, b) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (tuple(a, b)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((lower(c) AS lc)) TYPE tokenbf_v1(1024, 2, 0)');

DROP TABLE t04502;
