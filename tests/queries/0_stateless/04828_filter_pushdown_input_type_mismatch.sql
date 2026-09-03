-- Filter push-down into `system.tables` must not use a predicate subtree whose input types
-- do not match the block it is evaluated over: `information_schema.tables` is a view where
-- `engine` is `Nullable(String)`, while `system.tables` has `engine String`. Evaluating such
-- a predicate failed with a logical error (`Unexpected return type from toString.
-- Expected Nullable(String). Got String`).
-- Found by the AST fuzzer: https://github.com/ClickHouse/ClickHouse/issues/113982

-- The queries are restricted to the current database: without the restriction, `system.tables`
-- computes `total_rows` for every table on the server, and in a parallel test run this hits
-- other tests' half-dropped tables, whose errors are streamed to the client as server logs.
-- Create a table so that the filtered block the predicate is evaluated over is non-empty.
CREATE TABLE table_04828 (x UInt8) ENGINE = MergeTree ORDER BY x;

SELECT 1 FROM (SELECT 1 FROM information_schema.tables WHERE indexHint(toString(engine)) AND table_schema = currentDatabase()) FORMAT Null;

SELECT count() > 0 FROM (SELECT 1 FROM merge('information_schema', 'TABLES') WHERE indexHint(toString(engine)) AND table_schema = currentDatabase());

SELECT 1 FROM information_schema.tables WHERE indexHint(toString(engine)) AND table_schema = currentDatabase()
UNION ALL
SELECT 1 FROM information_schema.tables WHERE indexHint(toString(engine)) AND table_schema = currentDatabase()
FORMAT Null;

DROP TABLE table_04828;
