-- `APPLY (x -> <matcher>)` followed by another transformer produced an AST that did not survive
-- the format-parse-format round trip, tripping the `Inconsistent AST formatting` logical error in
-- debug builds. The formatter emitted the lambda without its brackets, so on re-parse the matcher
-- grammar absorbed the following transformer into the lambda body and one of the two
-- `ColumnsApplyTransformer` children was lost.
--
-- The errors below are the ordinary ones each statement raises once the round-trip check has
-- passed; they differ between analyzers, hence the alternatives in the hints.

-- Original reproducer: must not abort.
SELECT * APPLY (x -> COLUMNS('a')) APPLY toString FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Every matcher-valued lambda body, against every kind of following transformer.
SELECT * APPLY (x -> COLUMNS('a')) EXCEPT a FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> COLUMNS('a')) REPLACE (a AS a) FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> *) APPLY toString FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY (x -> *) EXCEPT a FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> *) REPLACE (a AS a) FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> t.*) APPLY toString FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY (x -> t.*) EXCEPT a FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> t.*) REPLACE (a AS a) FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> t.COLUMNS('a')) APPLY toString FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY (x -> t.COLUMNS('a')) EXCEPT a FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> t.COLUMNS('a')) REPLACE (a AS a) FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> COLUMNS('a') APPLY toString) APPLY toString FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY (x -> COLUMNS('a') APPLY toString) EXCEPT a FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
SELECT * APPLY (x -> COLUMNS('a') APPLY toString) REPLACE (a AS a) FROM (SELECT 1 AS a, 2 AS ab) AS t; -- { serverError UNSUPPORTED_METHOD, UNKNOWN_IDENTIFIER }
-- A single-column list matcher resolves under the analyzer, so this carrier executes rather than
-- erroring: the fix must let the query through, not merely turn the abort into an error.
SELECT * APPLY (x -> COLUMNS(a)) APPLY toString FROM (SELECT 1 AS a, 2 AS ab) AS t SETTINGS enable_analyzer = 1;
-- The matcher body alone never aborted; keep it as the negative control of the trigger.
SELECT * APPLY (x -> COLUMNS(a)) FROM (SELECT 1 AS a, 2 AS ab) AS t SETTINGS enable_analyzer = 1;

-- format(x) must equal format(format(x)) for every carrier; this is what the internal round-trip
-- check verifies.
SELECT formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\')) APPLY toString FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\')) APPLY toString FROM t'));
SELECT formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(a)) EXCEPT a FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(a)) EXCEPT a FROM t'));
SELECT formatQuerySingleLine('SELECT * APPLY (x -> *) REPLACE (a AS a) FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> *) REPLACE (a AS a) FROM t'));
SELECT formatQuerySingleLine('SELECT * APPLY (x -> t.*) APPLY toString FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> t.*) APPLY toString FROM t'));
SELECT formatQuerySingleLine('SELECT * APPLY (x -> t.COLUMNS(\'a\')) EXCEPT a FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> t.COLUMNS(\'a\')) EXCEPT a FROM t'));
SELECT formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\') APPLY toString) APPLY toString FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\') APPLY toString) APPLY toString FROM t'));
SELECT formatQuerySingleLine('SELECT * APPLY (x -> x + 1) FROM t') = formatQuerySingleLine(formatQuerySingleLine('SELECT * APPLY (x -> x + 1) FROM t'));

-- Concrete formatted output: the lambda keeps the brackets that delimit it from what follows.
SELECT formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\')) APPLY toString FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (x -> *) EXCEPT a FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (x -> t.*) REPLACE (a AS a) FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\') APPLY toString) APPLY length FROM t');
SELECT formatQuerySingleLine('SELECT * EXCEPT b APPLY (x -> x + 1) REPLACE (a AS a) FROM t');

-- The brackets belong to the lambda, so a non-matcher body gets them too.
SELECT formatQuerySingleLine('SELECT * APPLY (x -> x + 1) FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (x -> x) FROM t');

-- No over-reach: a bare function name stays unbracketed, and the `column_name_prefix` forms are
-- unchanged.
SELECT formatQuerySingleLine('SELECT * APPLY toString APPLY length FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY sum FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY quantile(0.5) FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (toString, \'p_\') FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (x -> f(x), \'p_\') FROM t');
SELECT formatQuerySingleLine('SELECT * APPLY (x -> COLUMNS(\'a\'), \'p_\') APPLY toString FROM t');
-- The sibling transformers do not carry a lambda and must format as before.
SELECT formatQuerySingleLine('SELECT * EXCEPT (a, b) FROM t');
SELECT formatQuerySingleLine('SELECT * EXCEPT STRICT a FROM t');
SELECT formatQuerySingleLine('SELECT * REPLACE (a + 1 AS a) FROM t');

-- Column names come from `appendColumnName`, not from the formatter, so they must not shift.
SELECT * APPLY (x -> x + 1) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x * 2) APPLY toString FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY toString FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
