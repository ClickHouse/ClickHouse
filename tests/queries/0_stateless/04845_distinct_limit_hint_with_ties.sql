-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: the asserted rows_before_limit_at_least depends on the read topology.

-- The DISTINCT early-stop limit hint must not be derived when the consumer needs rows beyond
-- limit + offset: WITH TIES has an unbounded tie suffix, exact_rows_before_limit counts
-- the whole stream, and WITH TOTALS accumulates over it.

SET max_block_size = 8192;

-- WITH TIES: every row ties on k, so all 200000 rows must come back.
SELECT count() FROM (SELECT DISTINCT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT DISTINCT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 3 WITH TIES);
SELECT count() FROM (SELECT DISTINCT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 1 OFFSET 1 WITH TIES);
-- WITH TIES without DISTINCT is unaffected.
SELECT count() FROM (SELECT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 1 WITH TIES);
-- An ordinary positive LIMIT still uses the hint and stays correct.
SELECT count() FROM (SELECT DISTINCT number FROM numbers(200000) ORDER BY number LIMIT 3);

SET enable_analyzer = 0;

SELECT count() FROM (SELECT DISTINCT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT DISTINCT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 3 WITH TIES);
SELECT count() FROM (SELECT DISTINCT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 1 OFFSET 1 WITH TIES);
SELECT count() FROM (SELECT number, 0 AS k FROM numbers(200000) ORDER BY k LIMIT 1 WITH TIES);
SELECT count() FROM (SELECT DISTINCT number FROM numbers(200000) ORDER BY number LIMIT 3);

SET enable_analyzer = 1;

-- exact_rows_before_limit needs a MergeTree table: numbers() reports the full count either way.
DROP TABLE IF EXISTS t_erbl;
CREATE TABLE t_erbl (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_erbl SELECT number FROM numbers(200000);

-- ORDER BY keeps the printed rows deterministic; without it the three returned rows depend on
-- read order, while rows_before_limit_at_least is what this case actually asserts.
SELECT DISTINCT n FROM t_erbl ORDER BY n LIMIT 3 FORMAT JSONCompact SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0;
SELECT DISTINCT n FROM t_erbl ORDER BY n LIMIT 3 FORMAT JSONCompact SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0, enable_analyzer = 0;

-- WITH TOTALS also forces the LIMIT to read till end, so the hint must not truncate the totals.
-- Each n is unique, so every group holds one row and the returned row is 1 whatever the read
-- order, while the totals row counts the whole table (200000) and shrinks if the stream is cut
-- short. The grouped stream must span more than one block for an early stop to be observable.
SELECT DISTINCT c FROM (SELECT n AS k, count() AS c FROM t_erbl GROUP BY k WITH TOTALS) LIMIT 1 FORMAT JSONCompact SETTINGS output_format_write_statistics = 0;
SELECT DISTINCT c FROM (SELECT n AS k, count() AS c FROM t_erbl GROUP BY k WITH TOTALS) LIMIT 1 FORMAT JSONCompact SETTINGS output_format_write_statistics = 0, enable_analyzer = 0;
-- Same for WITH TOTALS on the query itself rather than in a subquery in FROM.
SELECT DISTINCT count() FROM t_erbl GROUP BY n WITH TOTALS LIMIT 1 FORMAT JSONCompact SETTINGS output_format_write_statistics = 0, query_plan_remove_redundant_distinct = 0;
SELECT DISTINCT count() FROM t_erbl GROUP BY n WITH TOTALS LIMIT 1 FORMAT JSONCompact SETTINGS output_format_write_statistics = 0, query_plan_remove_redundant_distinct = 0, enable_analyzer = 0;
-- WITH TOTALS plus ORDER BY does not read till end, so the hint still applies and stays correct.
-- TSV prints the row and the totals but not rows_before_limit_at_least, which tracks max_threads.
SELECT DISTINCT c FROM (SELECT n AS k, count() AS c FROM t_erbl GROUP BY k WITH TOTALS) ORDER BY c LIMIT 1 FORMAT TabSeparatedWithNames;

DROP TABLE t_erbl;
