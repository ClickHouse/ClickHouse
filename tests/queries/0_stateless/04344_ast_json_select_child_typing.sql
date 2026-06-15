-- Regression tests for the AST JSON review hardening of parser-owned child types in SELECT/INSERT/
-- ALTER/dictionary nodes. These slots are parser-produced concrete nodes (or lists with concrete
-- element types) that downstream formatting/analysis downcasts, so `readJSON` now validates them and
-- malformed `clickhouse_json` fails closed with `BAD_ARGUMENTS` instead of reaching an internal cast.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO t SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a INNER JOIN b ON a.x = b.y'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ARRAY JOIN arr AS x'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (ADD STATISTICS a TYPE tdigest)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 GROUP BY 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 ORDER BY 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('WITH x AS (SELECT 1) SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(x -> (x + 1)) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT t.* FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(a, b) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM a INNER JOIN b USING (x)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM (SELECT 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`n` Nested(a UInt8, b String)) ENGINE = Memory'));

-- ---------------------------------------------------------------------------
-- `ASTInsertQuery.select` is an `ASTSelectWithUnionQuery` (insert execution downcasts it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('INSERT INTO t SELECT 1'), '"select":{"type":"SelectWithUnionQuery"', '"select":{"type":"Identifier","name":"s"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTTableExpression.database_and_table_name` is an `ASTTableIdentifier` (`StorageID` requires it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM db.t'), '"database_and_table_name":{"type":"TableIdentifier"', '"database_and_table_name":{"type":"Identifier","name":"t"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTTablesInSelectQueryElement.table_join` is an `ASTTableJoin` (`formatImpl` downcasts it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM a INNER JOIN b ON a.x = b.y'), '"table_join":{"type":"TableJoin"', '"table_join":{"type":"Identifier","name":"j"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTStatisticsDeclaration.columns` children are `ASTIdentifier` (`getColumnNames` downcasts them).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (ADD STATISTICS a TYPE tdigest)'), '"columns":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}', '"columns":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTSelectQuery` list slots (`group_by`/`order_by`/...) are `ASTExpressionList`s, and ORDER BY
-- children are `ASTOrderByElement`s.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 GROUP BY 1'), '"group_by":{"type":"ExpressionList"', '"group_by":{"type":"Identifier","name":"g"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 ORDER BY 1'), '"type":"OrderByElement"', '"type":"Identifier","name":"o"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTWithElement.subquery` must derive from `ASTWithAlias` (`formatImpl` `dynamic_cast`s it).
-- An `ASTExpressionList` is not an `ASTWithAlias`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('WITH x AS (SELECT 1) SELECT 1'), '"subquery":{"type":"Subquery"', '"subquery":{"type":"ExpressionList"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTColumnsApplyTransformer` lambda must carry the parser shape (argument tuple + body).
-- Emptying the lambda's `arguments` makes `transform`'s `arguments->children.at(1)` invalid.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * APPLY(x -> (x + 1)) FROM t'), '"name":"lambda","arguments":{"type":"ExpressionList","children":[', '"name":"lambda","arguments":{"type":"ExpressionList","children":[]},"unused":[')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTDictionaryAttributeDeclaration`: `attr_type` must be a data type, `name` non-empty.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE DICTIONARY d (`k` UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(FLAT()) LIFETIME(0)'), '"attr_type":{"type":"DataType"', '"attr_type":{"type":"Identifier","name":"x"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTQualifiedAsterisk.qualifier` is an `ASTIdentifier` (`QueryTreeBuilder` downcasts it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT t.* FROM t'), '"qualifier":{"type":"Identifier","name":"t"}', '"qualifier":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTColumnsListMatcher.column_list` children are `ASTIdentifier` (`QueryTreeBuilder` downcasts each).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT COLUMNS(a, b) FROM t'), '"column_list":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}', '"column_list":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTNameTypePair.name_type` is a data type (`Nested(a UInt8, ...)` produces `NameTypePair`s).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`n` Nested(a UInt8, b String)) ENGINE = Memory'), '"name_type":{"type":"DataType","name":"UInt8"}', '"name_type":{"type":"Identifier","name":"x"}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTTableExpression.subquery` is an `ASTSubquery`; `ASTTableJoin.using_expression_list` an `ASTExpressionList`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM (SELECT 1)'), '"subquery":{"type":"Subquery"', '"subquery":{"type":"ExpressionList"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM a INNER JOIN b USING (x)'), '"using_expression_list":{"type":"ExpressionList"', '"using_expression_list":{"type":"Identifier","name":"u"')); -- { serverError BAD_ARGUMENTS }
