-- `CREATE [HYPOTHETICAL] INDEX ... ((expr)) TYPE ...` aborted the server with an
-- `Inconsistent AST formatting` logical error in debug / sanitizer builds (issue #109163).
-- The index's own `(...)` already groups the expression, so an extra parenthesization of the
-- top-level expression could not survive the format-parse-format round trip and tripped the
-- internal consistency check. The fix strips that redundant parenthesization in the parser.

DROP TABLE IF EXISTS t04502;
CREATE TABLE t04502 (a UInt32, b UInt32, c String) ENGINE = MergeTree ORDER BY a;

-- Original reproducer: must not abort. `a` is not a function, so a plain UNKNOWN_FUNCTION
-- fires once the round-trip check passes cleanly.
CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a; -- { serverError UNKNOWN_FUNCTION }
CREATE INDEX i1 ON t04502 ((a())) TYPE a; -- { serverError UNKNOWN_FUNCTION }

-- format(x) must equal format(format(x)) for every extra-parenthesized index expression;
-- this is exactly what the internal round-trip check verifies.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + 1)) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a + 1)) TYPE minmax'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a())) TYPE a'));
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 (((a))) TYPE minmax') = formatQuerySingleLine(formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 (((a))) TYPE minmax'));

-- Concrete formatted output: the redundant parens are dropped.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a())) TYPE a');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (((a()))) TYPE a');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE HYPOTHETICAL INDEX i0 ON t04502 ((a + 1)) TYPE minmax');

-- The fix must not over-reach: a multi-element tuple keeps its parens (they are the value's
-- representation, not redundant grouping), and an alias keeps its required parens.
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((a, b)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 (tuple(a, b)) TYPE minmax');
SELECT formatQuerySingleLine('CREATE INDEX i0 ON t04502 ((lower(c) AS lc)) TYPE tokenbf_v1(1024, 2, 0)');

DROP TABLE t04502;
