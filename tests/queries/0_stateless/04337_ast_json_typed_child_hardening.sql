-- Regression tests for the AST JSON review hardening of typed child nodes and field
-- combinations: `readJSON` for several query types now restores parser-owned children with
-- concrete type checks (and rejects parser-impossible field combinations) at the JSON boundary,
-- so malformed `clickhouse_json` fails closed with `BAD_ARGUMENTS` instead of building an AST
-- that reaches a downstream invalid downcast / logical error or that formats into SQL that
-- disagrees with the operation actually executed.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO t SELECT * FROM s'));
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO FUNCTION remote(\'localhost\', system.one) SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO t FROM INFILE \'data.csv\' COMPRESSION \'gz\' FORMAT CSV'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TABLE a, b'));
SELECT formatQueryFromJSON(parseQueryToJSON('TRUNCATE TABLES FROM db'));
SELECT formatQueryFromJSON(parseQueryToJSON('TRUNCATE ALL TABLES FROM db'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t DRY RUN PARTS \'p1\', \'p2\''));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE INDEX idx ON t (col) TYPE minmax GRANULARITY 1'));

-- ---------------------------------------------------------------------------
-- INSERT: `table_function` must be an `ASTFunction`; `infile`/`compression` must be string
-- literals and `compression` requires `infile`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"InsertQuery","table_function":{"type":"Identifier","name":"input"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"InsertQuery","table_name":"t","infile":{"type":"Identifier","name":"f"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"InsertQuery","table_name":"t","infile":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"InsertQuery","table_name":"t","compression":{"type":"Literal","value":{"field_type":"String","value":"gz"}}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- TableExpression: `table_function` must be an `ASTFunction`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"TableExpression","table_function":{"type":"Identifier","name":"f"}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- DROP/TRUNCATE: `has_all`/`has_tables` only for `TRUNCATE TABLES FROM <db>`;
-- `database_and_tables` must be an `ASTExpressionList` of table identifiers.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop","database":"db","has_tables":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Truncate","database":"db","has_all":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop","database_and_tables":{"type":"Identifier","name":"t"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop","database_and_tables":{"type":"ExpressionList","children":[{"type":"Identifier","name":"t"}]}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- OPTIMIZE: `parts_list` only valid for `DRY RUN PARTS` and only as a non-empty list of
-- string literals; `DRY RUN` requires it.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"parts_list":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"String","value":"p"}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"dry_run":true}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"dry_run":true,"parts_list":{"type":"ExpressionList","children":[{"type":"Identifier","name":"p"}]}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- CREATE INDEX: `index_decl` must be an `ASTIndexDeclaration`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"CreateIndexQuery","index_name":{"type":"Identifier","name":"i"},"index_decl":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- CREATE WASM FUNCTION: `function_name` must be an `ASTIdentifier` and `arguments` must be an
-- `ASTExpressionList`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON('{"type":"CreateWasmFunctionQuery","function_name":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"CreateWasmFunctionQuery","function_name":{"type":"Identifier","name":"f"},"arguments":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- BACKUP: element-local fields must match the element type. A `TEMPORARY TABLE` element
-- (type 1) carrying `partitions` is parser-impossible and must be rejected (formatting drops
-- `PARTITIONS` while backup/restore would still honour it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('BACKUP TABLE t PARTITION \'p\' TO Disk(\'backups\', \'f\')'), '"type":0', '"type":1')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- The JSON nesting pre-scan budget is derived from `max_ast_depth` with headroom for the JSON
-- encoding overhead, so a valid AST whose depth is within `max_ast_depth` round-trips even when
-- its serialized JSON has more bracket levels than `max_ast_depth` (previously rejected).
-- ---------------------------------------------------------------------------
SET max_ast_depth = 20;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2 + 3 + 4 + 5'));
SET max_ast_depth = 1000;

-- ---------------------------------------------------------------------------
-- Review follow-up: more parser-owned children restored by concrete type.
-- ---------------------------------------------------------------------------

-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP FROM SNAPSHOT Disk(\'default\', \'/snapshot/\') TO Disk(\'default\', \'/backup/\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` Nullable(UInt8)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT quantile(0.9)(x)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8, PROJECTION p (SELECT x)) ENGINE = MergeTree ORDER BY x'));

-- ASTFunction: `arguments`/`parameters` are parser-owned `ASTExpressionList` children. A scalar
-- node would make `formatImpl` iterate its (empty) `children` and silently rewrite `f(x)` as `f()`.
SELECT formatQueryFromJSON('{"type":"Function","name":"f","arguments":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"f","parameters":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }

-- ASTDataType: `arguments` must be the `ASTExpressionList` produced by `ParserDataType`; a non-list
-- child is silently dropped (e.g. `Nullable(UInt8)` formatting as bare `Nullable`).
SELECT formatQueryFromJSON('{"type":"DataType","name":"Nullable","arguments":{"type":"Identifier","name":"UInt8"}}'); -- { serverError BAD_ARGUMENTS }

-- ASTSelectQuery: `tables` must be an `ASTTablesInSelectQuery`; analysis helpers downcast `tables()`.
SELECT formatQueryFromJSON('{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"tables":{"type":"Identifier","name":"system.one"}}'); -- { serverError BAD_ARGUMENTS }

-- ASTProjectionDeclaration: `projection_type` must be an `ASTFunction`.
SELECT formatQueryFromJSON('{"type":"ProjectionDeclaration","name":"p","index":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"}]},"projection_type":{"type":"Identifier","name":"minmax"}}'); -- { serverError BAD_ARGUMENTS }

-- ASTBackupQuery: `backup_name` must be the parser-owned `ASTFunction` (marked `BACKUP_NAME`).
SELECT formatQueryFromJSON(replace(parseQueryToJSON('BACKUP TABLE t TO Disk(\'backups\', \'f\')'), '"type":"Function","name":"Disk"', '"type":"Identifier","name":"Disk"')); -- { serverError BAD_ARGUMENTS }
