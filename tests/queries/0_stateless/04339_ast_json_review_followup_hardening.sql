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
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM view(SELECT 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count() OVER (ORDER BY x) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT any(x) IGNORE NULLS FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT quantile(0.9)(x) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM view(SELECT 1 SETTINGS max_threads = 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t GROUP BY GROUPING SETS ((a, b), (c))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(quantiles(0.5, 0.9))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * REPLACE (a + 1 AS a) FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION ALL (SELECT 2 UNION DISTINCT SELECT 3)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM view(SELECT 1 UNION ALL SELECT 2)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION ALL SELECT 2 FORMAT Null'));

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
-- ASTTableIdentifier: a table identifier has at most two parts (`database.table`); the parser
-- rejects more, and `getTableId` would otherwise mis-resolve a longer name.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM db.tbl'), '"name_parts":["db","tbl"]', '"name_parts":["db","tbl","extra"]')); -- { serverError BAD_ARGUMENTS }

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

-- ---------------------------------------------------------------------------
-- ASTFunction: a bare select query argument is parser-producible only in `view(SELECT ...)` and
-- `viewIfPermitted(SELECT ... ELSE f(...))`. One row per rejected carrier: another function name, an
-- extra argument, `parameters`, a nested expression list, then parameters / window / NULLS action /
-- query output options under `view` itself, and each other select query node type the format can
-- build. The `position()` row anchors the `replace()` rows against a serialization change.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"Function","name":"any","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"foo","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"foo","parameters":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"foo","arguments":{"type":"ExpressionList","children":[{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT position(parseQueryToJSON('SELECT * FROM view(SELECT 1)'), '"type":"Function","name":"view","arguments"') > 0;
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM view(SELECT 1)'), '"name":"view","arguments"', '"name":"view","parameters"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM view(SELECT 1)'), '"name":"view","arguments"', '"name":"view","is_window_function":true,"window_name":"w","arguments"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM view(SELECT 1)'), '"name":"view","arguments"', '"name":"view","nulls_action":"RESPECT_NULLS","arguments"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"any","arguments":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"foo","arguments":{"type":"ExpressionList","children":[{"type":"SelectIntersectExceptQuery","final_operator":"INTERSECT ALL","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}},{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}]}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"any","arguments":{"type":"ExpressionList","children":[{"type":"ProjectionSelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"format_ast":{"type":"Identifier","name":"Null"}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"settings_ast":{"type":"SetQuery","changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"out_file":{"type":"Literal","value":{"field_type":"String","value":"f"}}},{"type":"Function","name":"null","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"String","value":"x UInt8"}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- An expression position holds an expression, and neither an expression list nor a select query is
-- one. One row per boundary and carrier, over the three boundaries that own such a position: a
-- function's `arguments` and `parameters`, the `APPLY` transformer's own `parameters`, and the
-- `REPLACE` replacement child. A nested list is parser-producible elsewhere, in
-- `GROUP BY GROUPING SETS ((a, b), (c))`, which round-trips above.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"quantile","parameters":{"type":"ExpressionList","children":[{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Float64","value":0.5}}]}]},"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk","transformers":{"type":"ColumnsTransformerList","children":[{"type":"ColumnsApplyTransformer","func_name":"quantiles","parameters":{"type":"ExpressionList","children":[{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Float64","value":0.5}},{"type":"Literal","value":{"field_type":"Float64","value":0.9}}]}]}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk","transformers":{"type":"ColumnsTransformerList","children":[{"type":"ColumnsApplyTransformer","func_name":"quantiles","parameters":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk","transformers":{"type":"ColumnsTransformerList","children":[{"type":"ColumnsReplaceTransformer","children":[{"type":"ColumnsReplaceTransformerReplacement","name":"a","children":[{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}]}]}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk","transformers":{"type":"ColumnsTransformerList","children":[{"type":"ColumnsReplaceTransformer","children":[{"type":"ColumnsReplaceTransformerReplacement","name":"a","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}]}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- ASTSelectWithUnionQuery / ASTSelectIntersectExceptQuery: an element of a set operation chain is
-- never a one-select chain of its own (`ParserSelectWithUnionQuery` lifts it) and never carries
-- query output options (`ParserQueryWithOutput` attaches those to the outermost query only). One
-- row per carrier: the wrapper alone, the wrapper as the blessed `view` argument, then an element
-- carrying `FORMAT` under each of the two set operation nodes.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}},{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}},{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}]},"list_of_modes":["UNION_ALL"],"format_ast":{"type":"Identifier","name":"Null"}}]},"list_of_modes":["UNION_ALL"]}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectIntersectExceptQuery","final_operator":"INTERSECT ALL","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}},{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}]},"format_ast":{"type":"Identifier","name":"Null"}}]}]}}'); -- { serverError BAD_ARGUMENTS }
