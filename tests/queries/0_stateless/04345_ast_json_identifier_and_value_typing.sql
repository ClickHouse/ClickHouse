-- Regression tests for the AST JSON review hardening of (1) parser-owned database/table target slots,
-- which must be identifiers (`getDatabase`/`getTable` read them via `tryGetIdentifierNameInto`), and
-- (2) parser-owned string-literal slots (`COMMENT`/`COLLATE`/`PARTS`), which must carry a `String`
-- `Field` value, not merely be an `ASTLiteral`. Malformed `clickhouse_json` now fails closed with
-- `BAD_ARGUMENTS` at the JSON boundary instead of formatting parser-impossible SQL or reaching an
-- internal downcast.

-- ---------------------------------------------------------------------------
-- Valid shapes that the new validation must NOT reject (round-trip unchanged), including the
-- parameterized-identifier forms (`{tbl:Identifier}`), which are `ASTIdentifier`s carrying an
-- `ASTQueryParameter` child and so must still be accepted.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE db.t'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE {db:Identifier}.{t:Identifier}'));
SELECT formatQueryFromJSON(parseQueryToJSON('USE db'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM SYNC REPLICA db.t'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TABLE {tbl:Identifier}'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM SCHEDULE MERGE t PARTS \'p1\', \'p2\''));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM FLUSH DISTRIBUTED t SETTINGS max_threads = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x UInt8 COMMENT \'c\') ENGINE = Memory'));

-- ---------------------------------------------------------------------------
-- Database/table target slots must be identifiers (`ASTIdentifier` or a subclass). A non-identifier
-- node would format as one target while `getDatabase`/`getTable` resolve a different/empty one.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('OPTIMIZE TABLE db.t'), '"table":{"type":"Identifier","name":"t"}', '"table":{"type":"Literal","value":{"field_type":"String","value":"t"}}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('USE db'), '"database":{"type":"Identifier","name":"db"}', '"database":{"type":"Literal","value":{"field_type":"String","value":"db"}}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM SYNC REPLICA db.t'), '"table":{"type":"Identifier","name":"t"}', '"table":{"type":"Literal","value":{"field_type":"String","value":"t"}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `COMMENT` and `COLLATE` are parser-produced as *string* literals; downstream code does
-- `child->as<ASTLiteral &>().value.safeGet<String>()`, and a non-string literal would also format as
-- parser-impossible SQL (e.g. `COMMENT 1`). Reject the wrong value category, not just the wrong node type.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x UInt8 COMMENT \'c\') ENGINE = Memory'), '"comment":{"type":"Literal","value":{"field_type":"String","value":"c"}}', '"comment":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT x FROM t ORDER BY x COLLATE \'en_US\''), '"collation":{"type":"Literal","value":{"field_type":"String","value":"en_US"}}', '"collation":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `CREATE INDEX`: `index_name` (printed by `formatQueryImpl`) and `index_decl->name` (used by
-- `convertToASTAlterCommand`/`validateCreateIndexQuery`) must agree, so the displayed DDL cannot name
-- one index while the executed operation targets another.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE INDEX idx ON t (x) TYPE minmax'), '"index_name":{"type":"Identifier","name":"idx"}', '"index_name":{"type":"Identifier","name":"other"}')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `SYSTEM SCHEDULE MERGE ... PARTS`: `scheduled_merge_parts` must be a non-empty list of *string*
-- literals (`scheduleMerge` does `child->as<ASTLiteral &>().value.safeGet<String>()`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM SCHEDULE MERGE t PARTS \'p1\''), '"children":[{"type":"Literal","value":{"field_type":"String","value":"p1"}}]', '"children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM SCHEDULE MERGE t PARTS \'p1\''), '"children":[{"type":"Literal","value":{"field_type":"String","value":"p1"}}]', '"children":[]')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `SYSTEM ... SETTINGS`: `query_settings` is an `ASTSetQuery` (`InterpreterSystemQuery` reads
-- `query.query_settings->as<ASTSetQuery>()->changes`).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM FLUSH DISTRIBUTED t SETTINGS max_threads = 1'), '"query_settings":{"type":"SetQuery"', '"query_settings":{"type":"Identifier","name":"s"')); -- { serverError BAD_ARGUMENTS }
