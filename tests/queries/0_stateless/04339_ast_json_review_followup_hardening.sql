-- Regression tests for the follow-up AST JSON review hardening (PR #100412): more `readJSON`
-- paths now restore parser-owned children with concrete type checks and reject parser-impossible
-- field combinations at the JSON boundary, so malformed `clickhouse_json` fails closed with
-- `BAD_ARGUMENTS` instead of building an AST that reaches a downstream invalid downcast / logical
-- error or that formats into SQL disagreeing with the operation actually executed.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD COLUMN c UInt8'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN x ADD ENUM VALUES(\'b\' = 2)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t RENAME COLUMN a TO b'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY SETTING s = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE t SET a = 1 WHERE b = 2'));
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO t (a, b) SELECT 1, 2'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(x -> (x + 1))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(quantile(0.9))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT arrayMap(x -> (x + 1), [1, 2, 3])'));
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES FROM db'));
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME TABLE a TO b'));
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME DATABASE a TO b'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM DROP REPLICA \'r\''));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM DROP REPLICA \'r\' FROM ZKPATH \'/clickhouse/tables/01/\''));
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP FROM SNAPSHOT Disk(\'default\', \'/snapshot/\') TO Disk(\'default\', \'/backup/\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT lambda(tuple(), 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT arrayMap(() -> 1, [1])'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(lambda(1)(tuple(x), x + 1))'));

-- ---------------------------------------------------------------------------
-- ASTAlterCommand: parser-owned children are restored by concrete type. `col_decl` must be an
-- `ASTColumnDeclaration`, `column`/`rename_to` must be `ASTIdentifier`, `settings_changes` must be
-- an `ASTSetQuery` (downstream `AlterCommand::parse` downcasts them unconditionally).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t ADD COLUMN c UInt8'), '"col_decl":{"type":"ColumnDeclaration"', '"col_decl":{"type":"Identifier"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t RENAME COLUMN a TO b'), '"rename_to":{"type":"Identifier"', '"rename_to":{"type":"Function"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY SETTING s = 1'), '"settings_changes":{"type":"SetQuery"', '"settings_changes":{"type":"Identifier"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTUpdateQuery: `assignments` must be an `ASTExpressionList` whose children are all
-- `ASTAssignment` (`MutationCommand::parse` downcasts every child to `ASTAssignment`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('UPDATE t SET a = 1 WHERE b = 2'), '"type":"Assignment","column_name":"a"', '"type":"Identifier","name":"a"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"UpdateQuery","table":{"type":"Identifier","name":"t"},"assignments":{"type":"Identifier","name":"a"},"predicate":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTInsertQuery: `columns` must be an `ASTExpressionList`; `settings_ast` must be an `ASTSetQuery`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"InsertQuery","table_name":"t","columns":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"InsertQuery","table_name":"t","settings_ast":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTColumnsApplyTransformer: `lambda` must be an `ASTFunction`, `parameters` an `ASTExpressionList`
-- (the lambda path does `lambda->as<const ASTFunction &>()` and `parameters` feeds `ASTFunction`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * APPLY(x -> (x + 1))'), '"lambda":{"type":"Function","name":"lambda"', '"lambda":{"type":"Identifier","name":"lambda"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * APPLY(quantile(0.9))'), '"parameters":{"type":"ExpressionList"', '"parameters":{"type":"Identifier"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTShowTablesQuery: `from` must be an `ASTIdentifier` (execution extracts a name only from an
-- identifier; a non-identifier would format `SHOW TABLES FROM <expr>` while resolving an empty db).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SHOW TABLES FROM db'), '"from":{"type":"Identifier"', '"from":{"type":"Function"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTRenameQuery: the `*_table_ast`/`*_database_ast` fields must be `ASTIdentifier` (access checks,
-- `RenameDescription` and the query log read them only as identifier names).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('RENAME TABLE a TO b'), '"from_table_ast":{"type":"Identifier"', '"from_table_ast":{"type":"Function"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTTableIdentifier: a table identifier can have any number of parts (a hierarchical name
-- `db.tbl.extra`, see `DatabaseCatalog`), and it round-trips as such.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM db.tbl'), '"name_parts":["db","tbl"]', '"name_parts":["db","tbl","extra"]'));

-- ---------------------------------------------------------------------------
-- ASTSystemQuery: `is_drop_whole_replica` is parser-impossible together with a scoped DROP REPLICA
-- target (the interpreter takes the whole-replica branch while formatting prints the scoped form).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM DROP REPLICA \'r\' FROM ZKPATH \'/clickhouse/tables/01/\''), '"replica":"r"', '"replica":"r","is_drop_whole_replica":true')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTBackupQuery: `FROM SNAPSHOT` (base_snapshot_name) is parser-producible only for `BACKUP`;
-- a `RESTORE` carrying it would format parser-impossible SQL and restore an empty element set.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('BACKUP FROM SNAPSHOT Disk(\'default\', \'/snapshot/\') TO Disk(\'default\', \'/backup/\')'), '"BackupQuery","kind":"BACKUP"', '"BackupQuery","kind":"RESTORE"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTFunction: `kind` = `LAMBDA_FUNCTION` is parser-producible only together with the
-- `is_lambda_function` flag; reject the kind without the flag.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"Function","name":"f","kind":"LAMBDA_FUNCTION"}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTFunction: `is_lambda_function` marks the lambda definition shape `lambda(tuple(...), body)`,
-- the only shape the parser sets it on, and `QueryTreeBuilder` takes the flag as proof of that shape
-- ahead of the `isASTLambdaFunction` predicate; reject the flag on any other shape. One row per
-- shape: no `arguments` (under a name that is not `lambda`, then under `lambda`), one argument, a
-- first argument that is not a tuple, an argument tuple without its own argument list, an otherwise
-- well-formed lambda whose name is not `lambda`, three arguments, the `APPLY` transformer child
-- (whose own boundary check accepts a two-child lambda whose first argument is not a tuple), and an
-- argument tuple carrying its own parameter list, which no `tuple` parse produces.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"Function","name":"f","is_lambda_function":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"lambda","is_lambda_function":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"lambda","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"tuple","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"lambda","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"lambda","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"tuple"},{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"f","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"tuple","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}},{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"lambda","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"tuple","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}},{"type":"Identifier","name":"x"},{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * APPLY(x -> (x + 1))'), '"name":"tuple"', '"name":"nottuple"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"lambda","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"tuple","parameters":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":7}}]},"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}},{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
