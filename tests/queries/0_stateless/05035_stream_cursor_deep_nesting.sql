-- The `CURSOR` clause of `STREAM` is parsed by a helper that recurses directly instead of going
-- through `IParserBase::parse`, so `max_parser_depth` was not in effect and a deeply nested cursor
-- exhausted the thread stack. 26.7 has no `parseQueryToJSON`, so `formatQuery` drives the parser instead.

SELECT formatQuery(concat('SELECT * FROM t STREAM CURSOR ', repeat('{''a'': ', 100000), '10', repeat('}', 100000)))
SETTINGS max_query_size = 100000000; -- { serverError TOO_DEEP_RECURSION }

SELECT formatQuery('SELECT * FROM t STREAM CURSOR {''a'': {''b'': 10}}') IS NOT NULL;
