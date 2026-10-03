-- AST JSON boundary of a projection's explicit column list: malformed `clickhouse_json` must fail
-- closed rather than build an AST that reaches an invalid downcast downstream.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt64, PROJECTION p (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x'));

-- One list covers both the optional and explicit type forms.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (ts DateTime, id UInt64, PROJECTION p (ts CODEC(DoubleDelta), id UInt64 CODEC(NONE)) AS (SELECT ts, id ORDER BY ts)) ENGINE = MergeTree ORDER BY ts'));

-- A column list combined with `WITH SETTINGS`.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt64, PROJECTION p (x UInt64 CODEC(NONE)) AS (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 1024)) ENGINE = MergeTree ORDER BY x'));

SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt64, y UInt64, PROJECTION p INDEX x, y TYPE basic) ENGINE = MergeTree ORDER BY x'));

SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt64, y UInt64, PROJECTION p (WITH x AS a, y AS b SELECT a, b GROUP BY a, b)) ENGINE = MergeTree ORDER BY x'));

-- The JSON boundary must preserve the parser's column-list disambiguation for keyword names.
SELECT position(formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (with UInt64, PROJECTION p (with CODEC(NONE)) AS (SELECT with ORDER BY with)) ENGINE = MergeTree ORDER BY with')), 'PROJECTION p (`with` CODEC(NONE)) AS') > 0;

-- A parenthesized subquery and COLUMNS matcher are parser-produced expression roots.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt64, PROJECTION p (SELECT x WHERE (SELECT 1))) ENGINE = MergeTree ORDER BY x'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt64, PROJECTION p (SELECT x WHERE COLUMNS(''x''))) ENGINE = MergeTree ORDER BY x'));

-- ---------------------------------------------------------------------------
-- `ProjectionDeclaration`: `columns` must be a non-empty `ASTExpressionList` of
-- `ASTColumnDeclaration`, and it is only meaningful together with a `SELECT` query.
-- ---------------------------------------------------------------------------

-- An empty column list is not something the parser can produce.
SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","columns":{"type":"ExpressionList","children":[]},"query":{"type":"ProjectionSelectQuery"}}'); -- { serverError BAD_ARGUMENTS }

-- A non-declaration child would reach `as<ASTColumnDeclaration &>` as an invalid cast.
SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","columns":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"query":{"type":"ProjectionSelectQuery"}}'); -- { serverError BAD_ARGUMENTS }

-- A column list describes what a `SELECT` produces, so it cannot stand without one.
SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","columns":{"type":"ExpressionList","children":[{"type":"ColumnDeclaration","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }

-- A JSON-only separator must not format a projection list that SQL cannot parse back.
WITH
    parseQueryToJSON('CREATE TABLE t (x UInt64, y UInt64, PROJECTION p (x CODEC(NONE), y CODEC(NONE)) AS (SELECT x, y ORDER BY x)) ENGINE = MergeTree ORDER BY x') AS original,
    replaceOne(
        original,
        '"order_by":{"type":"Identifier","name":"x"}},"columns":{"type":"ExpressionList"',
        '"order_by":{"type":"Identifier","name":"x"}},"columns":{"type":"ExpressionList","separator":";"') AS malformed
SELECT formatQueryFromJSON(malformed); -- { serverError BAD_ARGUMENTS }

-- These lists all come from comma-list parsers. The generic ExpressionList JSON reader also
-- accepts other separators, so each projection-owned list must enforce its parser shape.
SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","index":{"type":"ExpressionList","separator":";","children":[{"type":"Identifier","name":"x"},{"type":"Identifier","name":"y"}]},"projection_type":{"type":"Function","name":"basic","no_empty_args":true}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","with":{"type":"ExpressionList","separator":";","children":[{"type":"Identifier","name":"x","alias":"a"},{"type":"Identifier","name":"y","alias":"b"}]},"select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","separator":";","children":[{"type":"Identifier","name":"x"},{"type":"Identifier","name":"y"}]}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"group_by":{"type":"ExpressionList","separator":";","children":[{"type":"Identifier","name":"x"},{"type":"Identifier","name":"y"}]}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[]}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","with":{"type":"ExpressionList","children":[]},"select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }

-- Projection clauses parsed as expressions cannot hold a bare list, query, declaration, or
-- another non-expression AST node. Parenthesized SELECT expressions use `ASTSubquery` instead.
SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"where":{"type":"ExpressionList","separator":";","children":[{"type":"Identifier","name":"a"},{"type":"Identifier","name":"b"}]}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"where":{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"y"}]}}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"where":{"type":"ColumnDeclaration","name":"y"}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"ColumnDeclaration","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","index":{"type":"ExpressionList","children":[{"type":"ColumnDeclaration","name":"x"}]},"projection_type":{"type":"Function","name":"basic","no_empty_args":true}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `formatQuery` fixpoint. A projection's column list is the only caller of
-- `ASTExpressionList::formatImplMultiline` other than `ASTCreateQuery`, so its indentation is not
-- otherwise covered.
-- ---------------------------------------------------------------------------
SELECT formatQuery('CREATE TABLE t (ts DateTime, id UInt64, PROJECTION p (ts CODEC(DoubleDelta), id UInt64 CODEC(NONE)) AS (SELECT ts, id ORDER BY ts)) ENGINE = MergeTree ORDER BY ts');

SELECT formatQuerySingleLine('CREATE TABLE t (x UInt64, PROJECTION p (x UInt64 CODEC(NONE)) AS (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 1024)) ENGINE = MergeTree ORDER BY x');

-- A single declared column is included: `expression_list_always_start_on_new_line` only affects it.
SELECT formatQuery(formatQuery('CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(NONE)) AS (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x'))
     = formatQuery('CREATE TABLE t (x UInt64, PROJECTION p (x CODEC(NONE)) AS (SELECT x ORDER BY x)) ENGINE = MergeTree ORDER BY x') AS multiline_is_fixpoint;

SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (x UInt64, PROJECTION p (x UInt64 CODEC(NONE)) AS (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 1024)) ENGINE = MergeTree ORDER BY x'))
     = formatQuerySingleLine('CREATE TABLE t (x UInt64, PROJECTION p (x UInt64 CODEC(NONE)) AS (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 1024)) ENGINE = MergeTree ORDER BY x') AS singleline_is_fixpoint;

-- The multiline form re-parses to the same AST as the single-line form.
SELECT formatQuerySingleLine(formatQuery('CREATE TABLE t (ts DateTime, id UInt64, PROJECTION p (ts CODEC(DoubleDelta), id UInt64 CODEC(NONE)) AS (SELECT ts, id ORDER BY ts)) ENGINE = MergeTree ORDER BY ts'))
     = formatQuerySingleLine('CREATE TABLE t (ts DateTime, id UInt64, PROJECTION p (ts CODEC(DoubleDelta), id UInt64 CODEC(NONE)) AS (SELECT ts, id ORDER BY ts)) ENGINE = MergeTree ORDER BY ts') AS multiline_reparses_to_same;
