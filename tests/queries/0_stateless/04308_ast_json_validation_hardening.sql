-- Malformed JSON AST input must be rejected with `BAD_ARGUMENTS` during
-- deserialization instead of building an invalid AST that fails (or changes
-- semantics) later during formatting/execution. Covers required-field and
-- enum validation added across many `readJSON` implementations, plus the
-- fail-closed handling of queries that have no faithful JSON representation.

-- Required children that `formatImpl` dereferences unconditionally:
SELECT formatQueryFromJSON('{"type":"SelectQuery"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"WithElement","name":"cte"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"WindowListElement","name":"w"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"NameTypePair","name":"x"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"QualifiedAsterisk"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"QueryParameter"}'); -- { serverError BAD_ARGUMENTS }

-- Enum-like string fields: unknown values must be rejected, not silently defaulted:
SELECT formatQueryFromJSON('{"type":"Function","name":"f","nulls_action":"BOGUS"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Function","name":"f","kind":"BOGUS"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ConstraintDeclaration","name":"c","constraint_type":"ASSUMEE","expr":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }

-- A window function without any window target:
SELECT formatQueryFromJSON('{"type":"Function","name":"count","is_window_function":true}'); -- { serverError BAD_ARGUMENTS }

-- Queries with no faithful JSON representation are rejected up front (fail-closed),
-- instead of producing JSON that cannot be read back:
SELECT parseQueryToJSON('GRANT SELECT ON *.* TO user'); -- { serverError BAD_ARGUMENTS }
SELECT parseQueryToJSON('CREATE USER u'); -- { serverError BAD_ARGUMENTS }
SELECT parseQueryToJSON('INSERT INTO t VALUES (1)'); -- { serverError BAD_ARGUMENTS }
