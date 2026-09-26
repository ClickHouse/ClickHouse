-- `ParserSubquery` stores exactly one `SelectWithUnionQuery` in a `Subquery` node: the plain form comes
-- from `ParserSelectWithUnionQuery`, which never yields any other node type, and the `EXPLAIN` / `VALUES`
-- forms are rewritten into `SELECT * FROM viewExplain(...)` / `SELECT * FROM SQLStandardValues(...)`.
-- Consumers rely on exactly that: `interpretSubquery` forwards `children.at(0)` to
-- `InterpreterSelectWithUnionQuery`, whose constructor dereferences `query_ptr->as<ASTSelectWithUnionQuery>()`
-- without a null check. A JSON payload holding anything else must therefore be rejected at the
-- deserialization boundary.

SET enable_json_ast_dialect = 1;

-- Positives: every parser-produced subquery form round-trips.

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT (SELECT 1)$$))
    = formatQuerySingleLine($$SELECT (SELECT 1)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT * FROM (SELECT 1 UNION ALL SELECT 2)$$))
    = formatQuerySingleLine($$SELECT * FROM (SELECT 1 UNION ALL SELECT 2)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT 1 IN (SELECT 1)$$))
    = formatQuerySingleLine($$SELECT 1 IN (SELECT 1)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT EXISTS (SELECT 1)$$))
    = formatQuerySingleLine($$SELECT EXISTS (SELECT 1)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT * FROM (EXPLAIN SELECT 1)$$))
    = formatQuerySingleLine($$SELECT * FROM (EXPLAIN SELECT 1)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT * FROM (VALUES (1, 'a'), (2, 'b'))$$))
    = formatQuerySingleLine($$SELECT * FROM (VALUES (1, 'a'), (2, 'b'))$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$WITH cte AS (SELECT 1) SELECT * FROM cte$$))
    = formatQuerySingleLine($$WITH cte AS (SELECT 1) SELECT * FROM cte$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT * FROM (SELECT 1 INTERSECT SELECT 1)$$))
    = formatQuerySingleLine($$SELECT * FROM (SELECT 1 INTERSECT SELECT 1)$$);

-- Negatives: a child the parser cannot produce is rejected instead of reaching the interpreters.

SELECT formatQueryFromJSON('{"type":"Subquery","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"Subquery","children":[{"type":"Identifier","name":"t"}]}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"Subquery","children":[{"type":"ExpressionList","children":[]}]}'); -- { serverError BAD_ARGUMENTS }

-- A bare `SelectQuery`, and an `ExplainQuery`, are parser-impossible in this slot as well: the parser
-- always wraps the former into a `SelectWithUnionQuery` and rewrites the latter into `viewExplain`.

SELECT formatQueryFromJSON('{"type":"Subquery","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"Subquery","children":[{"type":"ExplainQuery","kind":"QUERY_PLAN"}]}'); -- { serverError BAD_ARGUMENTS }

-- The same payload nested in a real query is rejected as well, so it cannot reach `interpretSubquery`.
-- The substitution is anchored on the `Subquery` node rather than on the union mode: the parser leaves
-- `union_mode` at `UNION_DEFAULT` and records `UNION ALL` in `list_of_modes`, so an anchor naming the
-- mode would match nothing and quietly turn the check below into a no-op. Assert the anchor first, so
-- that a change to the serialization fails here instead of silently weakening the check.

SELECT position(parseQueryToJSON($$SELECT * FROM (SELECT 1 UNION ALL SELECT 2)$$),
    '"type":"Subquery","children":[{"type":"SelectWithUnionQuery"') > 0;

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$SELECT * FROM (SELECT 1 UNION ALL SELECT 2)$$),
    '"type":"Subquery","children":[{"type":"SelectWithUnionQuery"',
    '"type":"Subquery","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}')); -- { serverError BAD_ARGUMENTS }
