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
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * EXCEPT (a, b) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * EXCEPT (\'re\') FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * EXCEPT (\'\') FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (COMMENT COLUMN x \'c\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP INDEX idx ON t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count() OVER (PARTITION BY x ORDER BY y)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (UPDATE x = 1 WHERE 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (RESET SETTING a)'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN PLAN actions = 1 SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE INDEX idx ON t (x) TYPE minmax'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE INDEX idx ON t (x) TYPE minmax GRANULARITY 3'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN AST SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CHECK TABLE db.t'));
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM db.t WHERE 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ORDER BY x COLLATE \'en_US\''));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ORDER BY x WITH FILL FROM 1 TO 10'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt8, PROJECTION p (SELECT x)) ENGINE = MergeTree ORDER BY x'));
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE t TO Disk(\'d\', \'/b/\') SETTINGS async = 1'));

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

-- ---------------------------------------------------------------------------
-- `ASTAsterisk.transformers` is an `ASTColumnsTransformerList`; `ASTColumnsExceptTransformer`'s
-- explicit column children are `ASTIdentifier`s (`buildColumnTransformers` downcasts each).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * EXCEPT (a) FROM t'), '"transformers":{"type":"ColumnsTransformerList"', '"transformers":{"type":"Identifier","name":"x"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * EXCEPT (a, b) FROM t'), '"ColumnsExceptTransformer","children":[{"type":"Identifier","name":"a"}', '"ColumnsExceptTransformer","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTAlterCommand.comment` (COMMENT COLUMN / MODIFY COMMENT) is an `ASTLiteral`, and
-- `ASTDropIndexQuery.index_name` is an `ASTIdentifier` (both downcast by `AlterCommands`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (COMMENT COLUMN x \'c\')'), '"comment":{"type":"Literal"', '"comment":{"type":"Identifier","name":"c"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('DROP INDEX idx ON t'), '"index_name":{"type":"Identifier"', '"index_name":{"type":"Literal","value":{"field_type":"String","value":"idx"}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTWindowDefinition.partition_by`, `ASTAlterCommand.update_assignments`, and `ASTExplainQuery.settings`
-- are parser-owned typed slots (`ASTExpressionList` / `ASTExpressionList` / `ASTSetQuery`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT count() OVER (PARTITION BY x ORDER BY y)'), '"partition_by":{"type":"ExpressionList"', '"partition_by":{"type":"Identifier","name":"p"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (UPDATE x = 1 WHERE 1)'), '"update_assignments":{"type":"ExpressionList"', '"update_assignments":{"type":"Identifier","name":"u"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('EXPLAIN PLAN actions = 1 SELECT 1'), '"kind":"EXPLAIN","settings":{"type":"SetQuery"', '"kind":"EXPLAIN","settings":{"type":"Identifier","name":"s"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTCreateIndexQuery.index_name` is an `ASTIdentifier`; `ASTAlterQuery.command_list` children are
-- `ASTAlterCommand`s; and `EXPLAIN AST` requires an explained query.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE INDEX idx ON t (x) TYPE minmax'), '"index_name":{"type":"Identifier","name":"idx"}', '"index_name":{"type":"Literal","value":{"field_type":"String","value":"idx"}}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (DROP COLUMN x)'), '{"type":"AlterCommand"', '{"type":"Asterisk"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN AST"}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- A negative (out-of-`UInt64`-range) value for an unsigned field (`getUInt`, e.g. index `GRANULARITY`)
-- is rejected with a controlled `BAD_ARGUMENTS` instead of an uncaught `Poco::RangeException`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE INDEX idx ON t (x) TYPE minmax GRANULARITY 3'), '"granularity":3', '"granularity":-1')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTCheckTableQuery.table`/`database` are identifiers (`getTable`/`getDatabase` read them as such),
-- and a structured `Literal` `Field` with an out-of-range value is rejected with `BAD_ARGUMENTS`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CHECK TABLE db.t'), '"table":{"type":"Identifier"', '"table":{"type":"Literal","value":{"field_type":"String","value":"t"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 5'), '{"field_type":"UInt64","value":5}', '{"field_type":"UInt64","value":-1}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTDeleteQuery.database`/`table` are `ASTIdentifier`s (`InterpreterDeleteQuery` reads them as such).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('DELETE FROM db.t WHERE 1'), '"table":{"type":"Identifier","name":"t"}', '"table":{"type":"Literal","value":{"field_type":"String","value":"t"}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTOrderByElement.collation` is parser-produced as a string `ASTLiteral` (sort-list building does
-- `getCollation()->as<ASTLiteral &>().value`), and the optional `fill_*` slots are only meaningful under
-- `WITH FILL` (the parser never attaches them otherwise).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT x FROM t ORDER BY x COLLATE \'en_US\''), '"collation":{"type":"Literal"', '"collation":{"type":"Identifier","name":"c"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT x FROM t ORDER BY x WITH FILL FROM 1 TO 10'), '"with_fill":true', '"with_fill":false')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTProjectionDeclaration.query` is the parser-owned `ASTProjectionSelectQuery`
-- (`ProjectionDescription::getProjectionFromAST` downcasts it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x UInt8, PROJECTION p (SELECT x)) ENGINE = MergeTree ORDER BY x'), '"query":{"type":"ProjectionSelectQuery"', '"query":{"type":"Identifier","name":"q"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTBackupQuery.settings` is an `ASTSetQuery` (`BackupSettings::fromAST` downcasts it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('BACKUP TABLE t TO Disk(\'d\', \'/b/\') SETTINGS async = 1'), '"settings":{"type":"SetQuery"', '"settings":{"type":"Identifier","name":"s"')); -- { serverError BAD_ARGUMENTS }
