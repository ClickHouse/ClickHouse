-- `parseQueryToJSON` walks the parsed query recursively, so a query nested deeper than that walk can
-- follow must be reported rather than crash the server. `max_parser_depth` is raised far above the
-- nesting so that the parser's own limit does not answer first, and the depths are far past what any
-- build's stack can hold, so the outcome does not depend on the build.

SELECT length(parseQueryToJSON(concat('SELECT ', repeat('abs(', 100), '1', repeat(')', 100)))) > 0;
SELECT length(parseQueryToJSON(concat('SELECT ', repeat('abs(', 40000), '1', repeat(')', 40000))))
SETTINGS max_parser_depth = 200000, max_ast_depth = 200000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError TOO_DEEP_RECURSION }

-- A single nested value nests the same way while adding one node to the query, so the depth of the
-- query does not bound it.
SELECT length(parseQueryToJSON(concat('SELECT ', repeat('[', 100), '1', repeat(']', 100)))) > 0;
SELECT length(parseQueryToJSON(concat('SELECT ', repeat('[', 500000), '1', repeat(']', 500000))))
SETTINGS max_parser_depth = 2000000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError TOO_DEEP_RECURSION }
