-- Coverage test for src/Analyzer/MatcherNode.cpp and src/Analyzer/ColumnTransformers.cpp:
--   MatcherNode.cpp:106-108  invalid COLUMNS regex → CANNOT_COMPILE_REGEXP
--   MatcherNode.cpp:282-298  qualified COLUMNS regexp in toASTImpl (t.COLUMNS('regex'))
--   MatcherNode.cpp:325-342  qualified COLUMNS list in toASTImpl (t.COLUMNS(col1, col2))
--   ColumnTransformers.cpp:233-237  ExceptColumnTransformerNode::toASTImpl regex branch (EXCEPT '^pattern')
-- All paths require enable_analyzer=1 (default) and EXPLAIN SYNTAX to trigger QueryTree→AST conversion.
-- Tags: no-parallel-replicas

SET enable_analyzer = 1; -- targeted code runs only in the analyzer path; pin it so old-analyzer CI shards behave the same
CREATE TABLE t_matcher (a UInt64, b_x UInt64, b_y UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_matcher VALUES (1, 2, 3);

-- 1. Invalid COLUMNS regex — hits MatcherNode.cpp:106-108 (CANNOT_COMPILE_REGEXP throw)
SELECT COLUMNS('[') FROM numbers(1); -- { serverError CANNOT_COMPILE_REGEXP }

-- 2. Qualified COLUMNS regexp — hits MatcherNode.cpp:282-298 in toASTImpl
--    (qualified_identifier.empty() == false, columns_matcher set)
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS('^b') FROM t_matcher;

-- 3. Qualified COLUMNS list — hits MatcherNode.cpp:325-342 in toASTImpl
--    (qualified_identifier.empty() == false, columns_identifiers set)
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS(b_x, b_y) FROM t_matcher;

-- 4. Qualified COLUMNS regexp + transformer — hits MatcherNode.cpp:291-295 (if (transformers) inside else branch)
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS('^b') APPLY(toString) FROM t_matcher;

-- 5. Qualified COLUMNS list + transformer — hits MatcherNode.cpp:335-339 (if (transformers) inside else branch)
EXPLAIN SYNTAX SELECT t_matcher.COLUMNS(b_x, b_y) APPLY(toString) FROM t_matcher;

-- 6. EXCEPT with regex string literal — hits ColumnTransformers.cpp:233-237
--    (ExceptColumnTransformerNode::toASTImpl with column_matcher set)
EXPLAIN SYNTAX SELECT * EXCEPT ('^b') FROM t_matcher;

DROP TABLE t_matcher;
