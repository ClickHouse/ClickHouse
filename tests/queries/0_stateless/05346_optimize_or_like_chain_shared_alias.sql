-- Random settings limits: optimize_or_like_chain=(1, None)

SET enable_identifier_resolve_cache = 1;

DROP TABLE IF EXISTS t_or_like_shared;
CREATE TABLE t_or_like_shared (s String, ns Nullable(String), ls LowCardinality(String), n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_or_like_shared SELECT toString(number % 3), if(number = 5, NULL, toString(number % 3)), toString(number % 3), number FROM numbers(12);

SELECT (s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%') AS r, count() FROM t_or_like_shared GROUP BY r HAVING r;
SELECT (s ILIKE '%A%' OR s ILIKE '%B%' OR s ILIKE '%C%' OR s ILIKE '%1%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (s LIKE 'a%' OR s LIKE 'b%' OR s LIKE 'c%' OR s LIKE 'd%' OR s LIKE 'e%' OR s LIKE 'f%' OR s LIKE 'g%' OR s LIKE 'h%' OR s LIKE 'i%' OR s LIKE '1%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (ns LIKE '%a%' OR ns LIKE '%b%' OR ns LIKE '%c%' OR ns LIKE '%1%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (ls LIKE '%a%' OR ls LIKE '%b%' OR ls LIKE '%c%' OR ls LIKE '%1%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%' OR ls LIKE '%a%' OR ls LIKE '%b%' OR ls LIKE '%c%' OR ls LIKE '%2%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%') AS r, count() FROM t_or_like_shared GROUP BY r WITH ROLLUP ORDER BY r, count();
WITH if(s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%', s, 'z') AS h SELECT (h LIKE '%a%' OR h LIKE '%b%' OR h LIKE '%c%' OR h LIKE '%1%') AS r FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%') AS r, indexHint(r) FROM t_or_like_shared GROUP BY r ORDER BY r;
SELECT (s LIKE '%a%' OR s LIKE '%b%' OR s LIKE '%c%' OR s LIKE '%1%') AS r, count() FROM t_or_like_shared GROUP BY r ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 0;

DROP TABLE t_or_like_shared;
