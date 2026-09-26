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
