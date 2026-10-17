-- A SELECT alias reused by GROUP BY, ORDER BY, HAVING, WHERE or another alias is rewritten the same way as the projection.
-- Random settings limits: optimize_and_compare_chain=(1, None); optimize_extract_common_expressions=(1, None)

SET enable_identifier_resolve_cache = 1;

DROP TABLE IF EXISTS t_shared_alias;
CREATE TABLE t_shared_alias (s String, n UInt64) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_shared_alias SELECT toString(number % 3), number FROM numbers(12);

SELECT (((s = '1') = 1) = 1) AND (n > 0) AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT (((s = '1') = 1) = 1) AND (n > 0) AS r FROM t_shared_alias GROUP BY r HAVING r OR NOT r ORDER BY r;
SELECT (((s = '1') = 1) = 1) AND (n > 0) AS r FROM t_shared_alias GROUP BY r WITH ROLLUP ORDER BY r;
SELECT (((s = '1') = 1) OR ((s = '1') = 1)) AND (n > 0) AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT and(and(n = 1 OR n = 2 OR n = 3, n >= 0), s = '1') AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT (n <= 3 AND n != 3 AND length(s) <= n) AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT (n <= 3 AND n != 3 AND length(s) <= n) AS r FROM t_shared_alias WHERE r OR NOT r GROUP BY r ORDER BY r;
SELECT (and(and(n > 1, n < 10), s = '1') OR and(n > 1, s = '2')) AS r FROM t_shared_alias WHERE r GROUP BY r ORDER BY r;
SELECT ((and(and(n > 1, n < 10), s = '1') OR and(n > 1, s = '2')) AND n < 100) AS r FROM t_shared_alias WHERE r GROUP BY r ORDER BY r;
SELECT (((s = '1') = 1) = 1) AND (n > 0) AS a, a AS b FROM t_shared_alias GROUP BY b ORDER BY b;
SELECT ((s = '1') = 1) AS x, (x = 1) AND (n > 0) AS a FROM t_shared_alias GROUP BY a, x ORDER BY a, x;
SELECT (n <= 3 AND n != 3) AS a, (a = 1) AS b FROM t_shared_alias GROUP BY a ORDER BY a;
SELECT (n <= 3 AND n != 3) AS a, (a = 1) AS b FROM t_shared_alias GROUP BY a ORDER BY a SETTINGS enable_identifier_resolve_cache = 0;
SELECT (n <= 3 AND n != 3) AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT (n < 3 AND n > 5) AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT (n = 1 OR n = 2 OR n = 3) AS r FROM t_shared_alias GROUP BY r ORDER BY r;
SELECT (a.n = b.n AND a.n > 2) AS r FROM t_shared_alias AS a JOIN t_shared_alias AS b ON r GROUP BY r ORDER BY r;
SELECT (n <= 3 AND n != 3 AND length(s) <= n) AS x, x AS y, (y AND s = '1') AS a FROM t_shared_alias GROUP BY y, a ORDER BY y, a;
SELECT (y AND s = '1') AS a, (n <= 3 AND n != 3 AND length(s) <= n) AS x, x AS y FROM t_shared_alias GROUP BY y, a ORDER BY y, a;
SELECT (x AND s = '1') AS a, (n <= 3 AND n != 3 AND length(s) <= n) AS x FROM t_shared_alias GROUP BY x, a ORDER BY x, a;
WITH (n <= 3 AND n != 3 AND length(s) <= n) AS x SELECT (x AND s = '1') AS a, x FROM t_shared_alias GROUP BY x, a ORDER BY x, a;

DROP TABLE t_shared_alias;
