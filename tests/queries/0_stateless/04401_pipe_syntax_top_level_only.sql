-- Pipe syntax (`|>`) is an experimental, top-level-only feature: `ParserPipelinedQuery` is
-- registered in the top-level query parser (`ParserQueryWithOutput` / `ParserExplainQuery`) but
-- not in the nested `SELECT`-accepting parsers (`ParserSubquery`, `ParserInsertQuery`,
-- `ParserCreateQuery`). This test pins that contract: a top-level pipe query is accepted, while the
-- same pipe shape inside a subquery, scalar subquery, CTE, `INSERT ... FROM`, or `CREATE VIEW ... AS`
-- is rejected with a syntax error.

SET allow_experimental_pipe_syntax = 1;

-- A top-level pipe query is accepted.
FROM numbers(3) |> WHERE number > 0;

-- A pipe query inside a subquery in `FROM` is not accepted (top-level only).
SELECT * FROM (FROM numbers(3) |> WHERE number > 0); -- { clientError SYNTAX_ERROR }

-- A pipe query inside a scalar subquery is not accepted.
SELECT (FROM numbers(3) |> WHERE number > 0 |> LIMIT 1); -- { clientError SYNTAX_ERROR }

-- A pipe query inside a CTE is not accepted.
WITH x AS (FROM numbers(3) |> WHERE number > 0) SELECT * FROM x; -- { clientError SYNTAX_ERROR }

-- A pipe query as an `INSERT ... FROM` source is not accepted.
CREATE TABLE t_pipe_top_level (x UInt64) ENGINE = Memory;
INSERT INTO t_pipe_top_level FROM numbers(3) |> WHERE number > 0; -- { clientError SYNTAX_ERROR }
DROP TABLE t_pipe_top_level;

-- A pipe query as a `CREATE VIEW ... AS` definition is not accepted.
CREATE VIEW v_pipe_top_level AS FROM numbers(3) |> WHERE number > 0; -- { clientError SYNTAX_ERROR }
