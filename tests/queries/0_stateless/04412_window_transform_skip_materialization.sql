-- `WindowTransform` no longer materializes pass-through `Const` / `LowCardinality` / `Sparse` columns.
-- Check that downstream operators (WHERE, ORDER BY, GROUP BY, UNION ALL) consuming such columns from a
-- window subquery still produce correct results.

SELECT c, lc, w FROM (SELECT 1 AS c, toLowCardinality(toString(number % 3)) AS lc, count() OVER () AS w FROM numbers(6)) ORDER BY lc, c, w;
SELECT lc, sum(w) FROM (SELECT toLowCardinality(toString(number % 3)) AS lc, count() OVER (PARTITION BY number % 3) AS w FROM numbers(9)) GROUP BY lc ORDER BY lc;
SELECT c, w FROM (SELECT 1 AS c, count() OVER () AS w FROM numbers(4)) WHERE c = 1 ORDER BY w;
SELECT c, w FROM (SELECT materialize(5) AS c, count() OVER () AS w FROM numbers(2) UNION ALL SELECT 7 AS c, 99 AS w) ORDER BY c, w;

-- A `Sparse`-serialized pass-through column from a MergeTree table.
DROP TABLE IF EXISTS t_window_sparse;
CREATE TABLE t_window_sparse (id UInt64, s UInt64) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO t_window_sparse SELECT number, if(number % 10 = 0, number, 0) FROM numbers(100);
SELECT s, w FROM (SELECT s, count() OVER () AS w FROM t_window_sparse) WHERE s > 0 ORDER BY s;
DROP TABLE t_window_sparse;
