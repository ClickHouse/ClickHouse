-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104170
-- Empty Unicode-quoted identifier “” should raise SYNTAX_ERROR,
-- not CANNOT_PARSE_QUOTED_STRING.

SET enable_analyzer=1;

SELECT 1 AS “”; -- { clientError SYNTAX_ERROR }
