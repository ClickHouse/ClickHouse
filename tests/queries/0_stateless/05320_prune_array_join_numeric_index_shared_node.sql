-- Numeric tupleElement indices over ARRAY JOIN of a Nested column with an unused subcolumn
-- must stay correct when ORDER BY ALL, GROUP BY ALL or an alias reuse the projection expression.

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (`n.a` Array(String), `n.b` Array(String), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES (['x', 'y'], ['abcde', 'fghij'], [5, 6]), ([], [], []), (['z'], ['klmno'], [9]);

SELECT tupleElement(n, 2), tupleElement(n, 3) FROM t_nested ARRAY JOIN n ORDER BY ALL;
SELECT tupleElement(n, 3), tupleElement(n, 2) FROM t_nested ARRAY JOIN n ORDER BY ALL;
SELECT tupleElement(n, 3), tupleElement(n, 2) FROM t_nested LEFT ARRAY JOIN n ORDER BY ALL DESC NULLS LAST;
SELECT tupleElement(n, 2), tupleElement(n, 3) FROM t_nested ARRAY JOIN n GROUP BY ALL ORDER BY 1;
SELECT * FROM (SELECT tupleElement(n, 3), tupleElement(n, 2) FROM t_nested ARRAY JOIN n GROUP BY ALL) ORDER BY 1 DESC;
SELECT tupleElement(n, 3) AS r, any(tupleElement(n, 2)) FROM t_nested ARRAY JOIN n GROUP BY r ORDER BY r SETTINGS enable_identifier_resolve_cache = 1;

DROP TABLE t_nested;
