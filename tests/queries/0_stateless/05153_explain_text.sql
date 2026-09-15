-- Basic formatting and one-row output with the `text` column.
EXPLAIN TEXT SELECT 1;
EXPLAIN TEXT (SELECT 1);
EXPLAIN TEXT SELECT 1 FROM t ONELINE, MULTILINE FORMAT JSONEachRow;
EXPLAIN TEXT SELECT 1 FROM t MULTILINE, ONELINE;

-- Ordered pagination actions.
EXPLAIN TEXT SELECT * FROM t LIMIT 100 MODIFY LIMIT 5, ONELINE;
EXPLAIN TEXT SELECT * FROM t LIMIT 10 PAGE 3, ONELINE;
EXPLAIN TEXT SELECT * FROM t LIMIT 10 OFFSET 50 PAGE 1, ONELINE;
EXPLAIN TEXT SELECT 1 LIMIT 10 MODIFY LIMIT 5, PAGE 3, MODIFY OFFSET 7, ONELINE;

-- Action-looking identifiers and incomplete candidate prefixes.
EXPLAIN TEXT SELECT modify FROM t MODIFY LIMIT 2, ONELINE;
EXPLAIN TEXT SELECT oneline FROM t ONELINE;
EXPLAIN TEXT SELECT oneline ONELINE;
EXPLAIN TEXT SELECT 1 AS oneline ONELINE;
SELECT notEmpty(formatQuerySingleLine('EXPLAIN TEXT SELECT 1 ONELINE, page'));

-- Source and outer output ownership.
EXPLAIN TEXT SELECT 1 FORMAT TSV MODIFY LIMIT 2, ONELINE FORMAT JSONEachRow;
EXPLAIN TEXT (SELECT 1 FORMAT TSV) MODIFY FORMAT CSV, ONELINE;
EXPLAIN TEXT (SELECT 1 UNION ALL SELECT 2) MODIFY FORMAT CSV, ONELINE;

-- Source parameters remain placeholders.
EXPLAIN TEXT SELECT * FROM t LIMIT {explain_text_size:UInt64} PAGE 2, ONELINE;
EXPLAIN TEXT SELECT * FROM t LIMIT {explain_text_size:UInt64} PAGE 3, ONELINE;

-- Unknown source settings remain printable.
EXPLAIN TEXT (SELECT 1 SETTINGS explain_text_unknown_setting = 1) ONELINE;

-- Missing action separators must not turn actions into source aliases.
SELECT formatQuery('EXPLAIN TEXT SELECT 1 MODIFY FORMAT CSV ONELINE'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('EXPLAIN TEXT SELECT 1 MODIFY LIMIT 5 ONELINE'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('EXPLAIN TEXT SELECT 1 ONELINE MULTILINE'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('EXPLAIN TEXT SELECT 1 LIMIT 9 MODIFY LIMIT 2 PAGE 3'); -- { serverError SYNTAX_ERROR }

-- Rewrite validation.
EXPLAIN TEXT SELECT 1 PAGE 2; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT SELECT 1 LIMIT 18446744073709551615 PAGE 3; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT (SELECT 1 UNION ALL SELECT 2) MODIFY LIMIT 5; -- { serverError BAD_ARGUMENTS }

-- SQL and JSON round trips preserve source/output ownership and actions.
WITH 'EXPLAIN TEXT (SELECT 1 FORMAT TSV) MODIFY LIMIT 5, PAGE 2, ONELINE FORMAT JSON' AS q
SELECT formatQuerySingleLine(formatQuerySingleLine(q)) = formatQuerySingleLine(q);

WITH 'EXPLAIN TEXT (SELECT 1 FORMAT TSV) MODIFY LIMIT 5, PAGE 2, ONELINE FORMAT JSON' AS q
SELECT formatQueryFromJSON(parseQueryToJSON(q)) = formatQuerySingleLine(q);

-- Unbound action parameters remain placeholders.
EXPLAIN TEXT SELECT 1 MODIFY LIMIT {explain_text_new_size:UInt64}, PAGE 3, ONELINE;

-- Supplied parameters must not substitute source or action placeholders.
SET param_explain_text_size = 99;
EXPLAIN TEXT SELECT 1 LIMIT {explain_text_size:UInt64} PAGE 2, ONELINE;
EXPLAIN TEXT SELECT 1 MODIFY LIMIT {explain_text_size:UInt64}, PAGE 3, ONELINE;

-- The same parameter is preserved in the source and resolved in outer settings.
SET param_explain_text_format = 'JSONEachRow';
EXPLAIN TEXT SELECT {explain_text_format:String} ONELINE
SETTINGS output_format = {explain_text_format:String};

-- Source settings are neither validated nor substituted.
EXPLAIN TEXT (SELECT 1 SETTINGS max_threads = 'not-a-number') ONELINE;
EXPLAIN TEXT (SELECT 1 SETTINGS max_threads = {explain_text_threads:UInt64}) ONELINE;

-- Construction settings must not rewrite the source.
EXPLAIN TEXT SELECT 1 ONELINE SETTINGS filter = '0';
EXPLAIN TEXT (SELECT 1 SETTINGS filter = '0') ONELINE;
EXPLAIN TEXT (SELECT * FROM (SELECT 1 SETTINGS filter = '0')) ONELINE;
EXPLAIN TEXT (SELECT 1 SETTINGS filter = '0' UNION ALL SELECT 2) ONELINE;

-- Outer settings still undergo normal validation.
EXPLAIN TEXT SELECT 1 ONELINE SETTINGS explain_text_unknown_setting = 1; -- { error UNKNOWN_SETTING }

-- Formatting also supports non-SELECT statements.
EXPLAIN TEXT SHOW TABLES ONELINE;

-- Parser-level restrictions.
SELECT formatQuery('EXPLAIN TEXT SELECT 1 PAGE 0'); -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('EXPLAIN TEXT oneline = 1 SELECT 1'); -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('SELECT * FROM (EXPLAIN TEXT SELECT 1)'); -- { serverError BAD_ARGUMENTS }
SELECT formatQuery('EXPLAIN TEXT INSERT INTO t VALUES (1)'); -- { serverError BAD_ARGUMENTS }

-- Exercise `viewExplain` itself, rather than only formatting its invocation.
SELECT * FROM viewExplain('EXPLAIN TEXT', '', (SELECT 1)); -- { serverError BAD_ARGUMENTS }

-- Remaining rewrite restrictions.
EXPLAIN TEXT (INSERT INTO t SELECT 1) MODIFY FORMAT CSV; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT (SELECT 1 UNION ALL SELECT 2) MODIFY OFFSET 5; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT (SELECT 1 UNION ALL SELECT 2) PAGE 2; -- { serverError BAD_ARGUMENTS }

-- Caller-authored action JSON must satisfy operand constraints.
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"UNKNOWN"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"ONELINE","operand":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"MULTILINE","operand":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"MODIFY LIMIT"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"MODIFY OFFSET"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"PAGE","operand":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainTextAction","kind":"MODIFY FORMAT","operand":{"type":"ExpressionList","children":[]}}'); -- { serverError BAD_ARGUMENTS }

-- An explicitly supplied action list cannot be empty.
SELECT formatQueryFromJSON(concat(
    '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":',
    parseQueryToJSON('SELECT 1'),
    ',"actions":{"type":"ExpressionList","children":[]}}'
)); -- { serverError BAD_ARGUMENTS }

-- Leading kind-specific settings remain forbidden through JSON.
SELECT formatQueryFromJSON(concat(
    '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":',
    parseQueryToJSON('SELECT 1'),
    ',"settings":',
    parseQueryToJSON('SET max_threads = 1'),
    '}'
)); -- { serverError BAD_ARGUMENTS }

-- Nested `EXPLAIN TEXT` preserves action ownership across formatting.
WITH 'EXPLAIN TEXT (EXPLAIN TEXT (SELECT 1) ONELINE) MULTILINE' AS q
SELECT parseQueryToJSON(formatQuerySingleLine(q)) = parseQueryToJSON(q);

-- A closing parenthesis terminates the inner action list.
SELECT
    parseQueryToJSON('EXPLAIN TEXT (EXPLAIN TEXT SELECT 1 ONELINE) MULTILINE')
    = parseQueryToJSON('EXPLAIN TEXT (EXPLAIN TEXT (SELECT 1) ONELINE) MULTILINE');

SELECT 'validation complete';
