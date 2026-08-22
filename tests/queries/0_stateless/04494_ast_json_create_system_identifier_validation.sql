-- Regression test for the AST JSON review hardening of CREATE / SYSTEM / identifier shapes.
-- Covers boundary validations that reject `clickhouse_json` payloads the SQL parser cannot produce:
--   * `CREATE TABLE` must not carry attach-only state (`attach_short_syntax`, `attach_from_path` /
--     `has_attach_from_path`, `attach_as_replicated`); these are gated behind `attach` in the parser and
--     `formatImpl` even asserts `attach || !has_attach_from_path`.
--   * `has_uuid` is not an independent input: it must match whether a non-`Nil` `uuid` is present,
--     otherwise `{uuid}` macro expansion would be enabled while the formatted SQL shows no `UUID` clause.
--   * `SYSTEM START/STOP LISTEN` `server_type.exclude_types` / `exclude_custom_names` elements must be
--     strictly typed (integers / strings), not coerced from the wrong JSON scalar type.
--   * The single child of an empty-name `ASTIdentifier` / `ASTTableIdentifier` (`{x:Identifier}`) must be
--     an `ASTQueryParameter`, since query-parameter visitors downcast it unconditionally.

-- ---------------------------------------------------------------------------
-- Valid shapes the strict validation must NOT reject.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (x Int) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('ATTACH TABLE t FROM \'/p\' (x Int) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t UUID \'a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5\' (x Int) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP LISTEN TCP'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP LISTEN QUERIES ALL EXCEPT TCP'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT {x:Identifier}'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 FROM {tbl:Identifier}'));

-- ---------------------------------------------------------------------------
-- Malformed JSON shapes that must fail closed with BAD_ARGUMENTS at the boundary.
-- ---------------------------------------------------------------------------

-- `attach_short_syntax` is only valid for ATTACH queries.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x Int) ENGINE = Memory'), '"attach_short_syntax":false', '"attach_short_syntax":true')); -- { serverError BAD_ARGUMENTS }
-- `has_attach_from_path` without an ATTACH (and without a path) is parser-impossible.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x Int) ENGINE = Memory'), '"has_attach_from_path":false', '"has_attach_from_path":true')); -- { serverError BAD_ARGUMENTS }
-- `attach_as_replicated` is only valid for ATTACH queries (inject it into a CREATE).
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x Int) ENGINE = Memory'), '"attach":false', '"attach":false,"attach_as_replicated":true')); -- { serverError BAD_ARGUMENTS }
-- `has_uuid` must match whether a non-Nil `uuid` is present.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (x Int) ENGINE = Memory'), '"has_uuid":false', '"has_uuid":true')); -- { serverError BAD_ARGUMENTS }

-- `server_type.exclude_types` must contain integers, not coerced strings.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM STOP LISTEN TCP'), '"server_type":{"type":', '"server_type":{"exclude_types":["x"],"type":')); -- { serverError BAD_ARGUMENTS }
-- `server_type.exclude_custom_names` must contain strings, not coerced numbers.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SYSTEM STOP LISTEN TCP'), '"server_type":{"type":', '"server_type":{"exclude_custom_names":[123],"type":')); -- { serverError BAD_ARGUMENTS }

-- The single child of an empty-name identifier must be an `ASTQueryParameter`, not a literal.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT {x:Identifier}'), '"type":"QueryParameter"', '"type":"Literal","value":{"field_type":"UInt64","value":1}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 FROM {tbl:Identifier}'), '"type":"QueryParameter"', '"type":"Literal","value":{"field_type":"UInt64","value":1}')); -- { serverError BAD_ARGUMENTS }
