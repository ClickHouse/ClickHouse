-- Tags: no-old-analyzer

-- Set operations are captured, and this pins that they are.
--
-- The reason it needs pinning: `isSupportedQuery` accepts `ASTSelectQuery` and
-- `ASTSelectWithUnionQuery`, and `IAST::as<T>` is an exact `typeid_cast`, so it does not accept
-- `ASTSelectIntersectExceptQuery` even though that class derives from `ASTSelectQuery`. Set
-- operations are captured anyway because the parser leaves that node one level down --
-- `SelectWithUnionQuery -> ExpressionList -> SelectIntersectExceptQuery` -- and nothing between
-- the parser and `canEnableProfiler` unwraps the single-element union into it. `InterpreterFactory`
-- routes on the same outer type and so reaches `InterpreterSelectQueryAnalyzer`.
--
-- Should normalization ever start unwrapping, the top-level AST would become the intersect node,
-- `isSupportedQuery` would reject it, and these queries would silently lose their plans -- and be
-- told by the decline trace that they are not `SELECT` queries. This test is what would notice.

SET log_query_plans = 1;

SELECT number FROM numbers(10) INTERSECT SELECT number FROM numbers(5)
    SETTINGS log_comment = '05185_intersect' FORMAT Null;

SELECT number FROM numbers(10) EXCEPT SELECT number FROM numbers(5)
    SETTINGS log_comment = '05185_except' FORMAT Null;

SELECT number FROM numbers(10) INTERSECT DISTINCT SELECT number FROM numbers(5)
    SETTINGS log_comment = '05185_intersect_distinct' FORMAT Null;

SELECT number FROM numbers(10) INTERSECT SELECT number FROM numbers(5) UNION ALL SELECT 7
    SETTINGS log_comment = '05185_mixed' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05185_', '') AS shape,
    -- A plan was stored, and it is the set operation's own plan: the step that performs it is there
    -- along with a source per branch.
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')) > 0 AS captured,
    countSubstrings(toJSONString(query_plan), '"Node Type":"IntersectOrExcept"') AS set_op_steps,
    countSubstrings(toJSONString(query_plan), '"Node Type":"ReadFromSystemNumbers"') AS sources
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05185\_%'
ORDER BY shape;
