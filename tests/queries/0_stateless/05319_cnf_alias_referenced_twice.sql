-- A SELECT alias used more than once in WHERE or HAVING, with the conversion to conjunctive normal form.
SET enable_identifier_resolve_cache = 1;
SET convert_query_to_cnf = 1;

DROP TABLE IF EXISTS t_cnf_alias;
CREATE TABLE t_cnf_alias (s String, n UInt64) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_cnf_alias SELECT toString(number % 3), number FROM numbers(12);

SELECT 'and', (n > 1 AND s = '1') AS r FROM t_cnf_alias WHERE r AND r GROUP BY r;
SELECT 'or not', n, (n > 1 AND s = '1') AS r FROM t_cnf_alias WHERE r OR (NOT r AND n = 0) ORDER BY n;
SELECT 'two groups', n, (n > 1 AND s = '1') AS r FROM t_cnf_alias WHERE (r OR n = 0) AND (NOT r OR n = 1) ORDER BY n;
SELECT 'distribute', n, (n > 1 OR s = '1') AS r FROM t_cnf_alias WHERE NOT r OR (r AND n = 11) ORDER BY n;
SELECT 'having', s, (count() > 1 AND s = '1') AS r FROM t_cnf_alias GROUP BY s HAVING r AND r;
SELECT 'tautology', count() FROM (SELECT (n > 5) AS a FROM t_cnf_alias WHERE a OR NOT a);
SELECT 'contradiction', count() FROM (SELECT (n > 5) AS a FROM t_cnf_alias WHERE NOT a AND (a OR n = 11));
SELECT 'mixed', n, (s = '1') AS a FROM t_cnf_alias WHERE (a OR n < 2) AND (NOT a OR n > 9) ORDER BY n;
SELECT 'inside a comparison', count() FROM (SELECT (n > 5) AS a FROM t_cnf_alias WHERE NOT a AND (a OR a = materialize(1)));
SELECT 'negated inside a comparison', count() FROM (SELECT (n > 1 AND s = '1') AS r FROM t_cnf_alias WHERE NOT r AND (r = 0 OR n = 11));
SELECT 'distributed inside a comparison', n, (n > 1 AND s = '1') AS r FROM t_cnf_alias WHERE (n = 0 OR r) AND (r = 1 OR n = 11) ORDER BY n;

DROP TABLE t_cnf_alias;
