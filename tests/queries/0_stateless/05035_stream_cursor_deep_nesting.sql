-- The `CURSOR` clause of `STREAM` is parsed by a helper that recurses directly instead of going
-- through `IParserBase::parse`, so `max_parser_depth` was not in effect and a deeply nested cursor
-- exhausted the thread stack.

SELECT parseQueryToJSON(concat('SELECT * FROM t STREAM CURSOR ', repeat('{''a'': ', 100000), '10', repeat('}', 100000)))
SETTINGS max_query_size = 100000000; -- { serverError TOO_DEEP_RECURSION }

SELECT parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': {''b'': 10}}') IS NOT NULL;
