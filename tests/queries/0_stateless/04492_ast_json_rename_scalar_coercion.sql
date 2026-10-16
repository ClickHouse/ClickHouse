-- Regression test for the AST JSON review hardening of nested non-AST scalar fields. The scalar
-- fields of a `RenameQuery` element (`from_database`, `from_table`, `to_database`, `to_table`,
-- `if_exists`) and of sibling non-AST structs were read through `Poco::getValue`, which coerces
-- scalar types: a string `"yes"` becomes a real `true`, and non-string names are stringified. That
-- let malformed `clickhouse_json` deserialize into a *different* valid AST instead of failing closed.
-- They are now read through `JSONObjectReader`, which validates the exact JSON scalar type and rejects
-- a wrong type with `BAD_ARGUMENTS` at the boundary.

-- ---------------------------------------------------------------------------
-- Valid shapes the strict validation must NOT reject (round-trip unchanged).
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME TABLE a TO b'));
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME DATABASE db1 TO db2'));
SELECT formatQueryFromJSON(parseQueryToJSON('RENAME TABLE db1.a TO db2.b'));
SELECT formatQueryFromJSON(parseQueryToJSON('SET max_threads = 4'));

-- ---------------------------------------------------------------------------
-- `RenameQuery` element `if_exists` is a JSON boolean. A string like `"yes"` must be rejected, not
-- coerced into a real `true` flag (which would silently change `RENAME` semantics). This is the
-- exact malformed shape called out in review.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('RENAME TABLE a TO b'), '"if_exists":false', '"if_exists":"yes"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('RENAME TABLE a TO b'), '"if_exists":false', '"if_exists":1')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- `RenameQuery` element name fields are JSON strings. A number (or boolean) must be rejected instead
-- of being stringified into a rename target.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('RENAME TABLE a TO b'), '"from_table":"a"', '"from_table":123')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('RENAME TABLE a TO b'), '"to_table":"b"', '"to_table":true')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('RENAME DATABASE db1 TO db2'), '"from_database":"db1"', '"from_database":123')); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- Sibling non-AST struct: an `ASTSetQuery` change `name` is a JSON string and must reject a number
-- rather than coercing it into a setting name.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SET max_threads = 4'), '"name":"max_threads"', '"name":123')); -- { serverError BAD_ARGUMENTS }
