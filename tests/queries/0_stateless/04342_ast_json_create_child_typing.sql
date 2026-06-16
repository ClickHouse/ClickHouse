-- Regression tests for the AST JSON review hardening of parser-owned child types in the
-- CREATE-family AST nodes (and a few others). These nodes hold children the SQL parser only ever
-- produces as one concrete type, and many members are concrete typed pointers whose `set` raises a
-- `LOGICAL_ERROR` on a wrong type. `readJSON` now restores them with `readChildOfType` (and validates
-- list element types / mutually-exclusive flags), so malformed `clickhouse_json` fails closed with
-- `BAD_ARGUMENTS` instead of an internal cast error or a parser-impossible AST.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged):
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8 COMMENT \'c\') ENGINE = Memory'));
-- A non-trivial (non-`ASTDataType`) column type still round-trips: `data_type` is intentionally left
-- untyped because a type can be an `ASTDataType`/`ASTTupleDataType`/etc.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` Tuple(UInt8, String)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory COMMENT \'c\''));
-- `MATERIALIZED VIEW ... TO <target>`: the `ViewTargets` `table_id` is serialized by its parts, so a
-- database-less target round-trips (it previously threw in `getFullTableName`), as does a `db.table` one.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO db2.dst AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t AS remote(\'addr\', \'db\', \'tbl\')'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE DICTIONARY d (`k` UInt64, `v` String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(FLAT()) LIFETIME(0)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count() OVER (ORDER BY 1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION ALL SELECT 2'));

-- ---------------------------------------------------------------------------
-- `ASTColumns`: the `columns` list children are `ASTColumnDeclaration`s; `getColumnsDescription`
-- downcasts each. A non-declaration child would format as a column while execution sees none.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'), '"type":"ColumnDeclaration"', '"type":"Identifier"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTStorage`: `engine` is an `ASTFunction`, `settings` an `ASTSetQuery`. Wrong types reach `set`
-- as a `LOGICAL_ERROR` cast failure unless rejected here.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'), '"engine":{"type":"Function"', '"engine":{"type":"Identifier"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1'), '"settings":{"type":"SetQuery"', '"settings":{"type":"Identifier","name":"x"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTCreateQuery`: `storage` is an `ASTStorage` typed member.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'), '"storage":{"type":"Storage"', '"storage":{"type":"Identifier","name":"s"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTCreateQuery`: `is_ordinary_view`/`is_materialized_view`/`is_window_view`/`is_dictionary` are
-- mutually exclusive query kinds. Setting two at once makes formatting and execution disagree.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE VIEW v AS SELECT 1'), '"is_materialized_view":false', '"is_materialized_view":true')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTColumnDeclaration`: `comment` is an `ASTLiteral` (`InterpreterCreateQuery` does `comment->as<ASTLiteral &>()`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8 COMMENT \'c\') ENGINE = Memory'), '"comment":{"type":"Literal"', '"comment":{"type":"Identifier","name":"c"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTFunction`: `window_definition` is an `ASTWindowDefinition` (`QueryTreeBuilder::buildWindow` downcasts it).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT count() OVER (ORDER BY 1)'), '"window_definition":{"type":"WindowDefinition"', '"window_definition":{"type":"Identifier","name":"w"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTSelectWithUnionQuery`: every `list_of_selects` element must be a select-form node.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 UNION ALL SELECT 2'), '"type":"SelectQuery"', '"type":"Identifier","name":"s"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTFunctionWithKeyValueArguments` (dictionary `SOURCE(...)`): `elements` is an `ASTExpressionList`
-- of `ASTPair`; `buildConfigurationFromFunctionWithKeyValueArguments` downcasts each child to `ASTPair`.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE DICTIONARY d (`k` UInt64, `v` String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(FLAT()) LIFETIME(0)'), '"type":"Pair"', '"type":"Identifier","name":"p"')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTCreateQuery.comment` is an `ASTLiteral` (`StorageFactory`/`DatabaseFactory` read it as a string).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory COMMENT \'c\''), '"comment":{"type":"Literal","value":{"field_type":"String","value":"c"}}', '"comment":{"type":"Identifier","name":"c"}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `ASTCreateQuery.as_table_function` is an `ASTFunction` (`setEngine` reads its `name`), and the
-- `dictionary_attributes_list` children are `ASTDictionaryAttributeDeclaration`s.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t AS remote(\'addr\', \'db\', \'tbl\')'), '"as_table_function":{"type":"Function"', '"as_table_function":{"type":"Identifier","name":"f"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE DICTIONARY d (`k` UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(FLAT()) LIFETIME(0)'), '"dictionary_attributes_list":{"type":"ExpressionList","children":[{"type":"DictionaryAttributeDeclaration"', '"dictionary_attributes_list":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"')); -- { serverError BAD_ARGUMENTS }
