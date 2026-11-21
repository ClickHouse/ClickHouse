-- Validation for `formatQueryFromJSON` on `UseQuery` and `Assignment` payloads.
-- `UseQuery` requires a `database` child, and `Assignment` requires exactly one
-- expression child — malformed input must throw `BAD_ARGUMENTS`, not produce an
-- invalid AST that later crashes during formatting.

-- UseQuery: missing required `database` field.
SELECT formatQueryFromJSON('{"type":"UseQuery"}'); -- { serverError BAD_ARGUMENTS }

-- UseQuery: well-formed payload still works.
SELECT formatQueryFromJSON('{"type":"UseQuery","database":{"type":"Identifier","name":"mydb"}}');

-- Assignment: empty children — must be rejected.
SELECT formatQueryFromJSON('{"type":"Assignment","column_name":"a","children":[]}'); -- { serverError BAD_ARGUMENTS }

-- Assignment: more than one child — must be rejected.
SELECT formatQueryFromJSON('{"type":"Assignment","column_name":"a","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}'); -- { serverError BAD_ARGUMENTS }
