-- Tags: no-parallel
-- Tag no-parallel: SQL user-defined functions are global (not per-database), so the fixed
-- function names would collide across concurrent runs of this test (e.g. in the flaky check)
-- with FUNCTION_ALREADY_EXISTS.

-- Regression test: a stateful function (`neighbor`) or `arrayJoin` hidden behind a SQL UDF
-- wrapper must not bypass the stateful / `arrayJoin` fences of the trivial-LIMIT source
-- optimizations. Both analyzer paths inline SQL UDFs before the checks run (`TreeRewriter` on
-- the AST path, function resolution on the analyzer path), and the AST-side detectors in
-- `InterpreterSelectQuery`, `numbersLikeUtils`, and `MergeTreeWhereOptimizer` additionally
-- descend into SQL UDF bodies. See https://github.com/ClickHouse/ClickHouse/pull/110188.

SET allow_deprecated_error_prone_window_functions = 1;
SET max_threads = 1;
-- `neighbor` results are block-dependent; pin the block size and keep the read single-stream so
-- the reference values do not depend on randomized settings.
SET max_block_size = 65536;
SET enable_parallel_replicas = 0;

DROP FUNCTION IF EXISTS f_04631;
DROP FUNCTION IF EXISTS explode_04631;

CREATE FUNCTION f_04631 AS x -> neighbor(x, 1);
CREATE FUNCTION explode_04631 AS a -> arrayJoin(a);

-- A capped one-row read would return 0 (`neighbor` sees only the row [0]); the full block gives 1.
SELECT f_04631(number) FROM numbers(3) LIMIT 1 SETTINGS enable_analyzer = 0;
SELECT f_04631(number) FROM numbers(3) LIMIT 1 SETTINGS enable_analyzer = 1;

-- `arrayJoin` drops rows with empty arrays: capping the source at LIMIT rows would yield fewer
-- than 5 output rows (only the multiples of 3 among the first 5 numbers).
SELECT explode_04631(if(number % 3 = 0, [number], [])) FROM numbers(100) LIMIT 5 SETTINGS enable_analyzer = 0;
SELECT explode_04631(if(number % 3 = 0, [number], [])) FROM numbers(100) LIMIT 5 SETTINGS enable_analyzer = 1;

-- A stateful UDF in the WHERE clause must keep `MergeTreeWhereOptimizer` from moving the
-- deterministic sibling conjunct to PREWHERE (it would prune rows before `neighbor` runs).
DROP TABLE IF EXISTS t_04631;
CREATE TABLE t_04631 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04631 SELECT number, number FROM numbers(10);
-- `neighbor(v, 1) = v + 1` holds for every row of the full block [0..9] with `k < 9` (the last
-- row is excluded by the sibling conjunct, not by block truncation), so the count is 9. If the
-- sibling conjunct were moved to PREWHERE, the block would be pre-filtered to [0..8] and the last
-- row would see the `neighbor` default 0 instead of 9, giving 8.
SELECT count() FROM t_04631 WHERE k < 9 AND f_04631(v) = v + 1 SETTINGS enable_analyzer = 0;
SELECT count() FROM t_04631 WHERE k < 9 AND f_04631(v) = v + 1 SETTINGS enable_analyzer = 1;
DROP TABLE t_04631;

DROP FUNCTION f_04631;
DROP FUNCTION explode_04631;
