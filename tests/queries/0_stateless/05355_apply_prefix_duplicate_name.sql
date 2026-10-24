-- An `APPLY (f, 'prefix')` name of a SELECT element is an alias of the result column, a nested one is an unused alias.
-- https://github.com/ClickHouse/ClickHouse/pull/115581

DROP TABLE IF EXISTS t_05355;
CREATE TABLE t_05355 (n UInt8) ENGINE = Memory;
INSERT INTO t_05355 VALUES (0), (1);

SELECT * APPLY (toTypeName, 'f_'), * APPLY (toString, 'f_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (toTypeName, 'f_'), * APPLY (x -> toString(x), 'f_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (toString, 'f_'), toTypeName(a) AS f_a FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT toTypeName(a) AS f_a, * APPLY (toString, 'f_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a) WHERE (a + 1 AS f_a) > 0; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (toString, 'p_') APPLY (upper, 'q_'), * APPLY (toTypeName, 'q_p_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (untuple, 'f_'), toString(tupleElement(a, 1)) AS `f_a.1` FROM (SELECT (1, 2) AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> x AS f_a SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT tuple(* APPLY (toString, 'p_')) AS f_a, * APPLY (toTypeName, 'f_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (toString, 'f_'), tuple(toString(a) AS f_a), tuple(toTypeName(a) AS f_a) FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * APPLY (toTypeName, 'f_') AS l1, x -> * APPLY (toString, 'f_') AS l2 SELECT l1(1), l2(1) FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
WITH x -> * APPLY (toString, 'f_') AS l1 SELECT l1(1), toTypeName(a) AS f_a FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (x -> x, 'f_') APPLY (x -> x), * APPLY (toTypeName, 'f_') FROM (SELECT 1 AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT r.* APPLY (toString, 'f_'), toString(l.number) AS `f_r.number` FROM numbers(2) AS l CROSS JOIN numbers(2) AS r; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * APPLY (toString, 'f_') FROM t_05355, t_05355, t_05355; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT * APPLY (toString, 'f_'), * APPLY (toString, 'f_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x, 'f_') APPLY (x -> x), * APPLY (x -> x, 'f_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (toTypeName, 'f_'), toTypeName(a) AS f_a FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> (SELECT 1), 'f_'), (SELECT 1) AS f_a FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT r.* APPLY (toString, 'f_'), toString(r.number) AS `f_r.number` FROM numbers(1) AS l CROSS JOIN numbers(1) AS r FORMAT TSVWithNames;
SELECT * APPLY (x -> arrayMap(y -> y + x, [1]), 'f_'), * APPLY (x -> arrayMap(y -> y + x, [1]), 'f_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;

SELECT toString(a) AS f_a, tuple(* APPLY (toTypeName, 'f_')) FROM (SELECT 1 AS a) FORMAT TSVWithNames;
WITH b_number AS a_number, 42 AS b_number SELECT tuple(* APPLY (toString, 'a_')), tuple(* APPLY (toTypeName, 'b_')) FROM numbers(1) FORMAT TSVWithNames;
SELECT toString(a) AS f_a FROM (SELECT [1] AS a) ARRAY JOIN COLUMNS('a') APPLY (x -> x, 'f_') FORMAT TSVWithNames;
SELECT arrayMap(x -> tuple(* APPLY (toTypeName, 'f_')), [1]) AS p, toString(a) AS f_a FROM (SELECT 1 AS a) FORMAT TSVWithNames;
WITH x -> * APPLY (toString, 'f_') AS l1 SELECT l1(1) FROM (SELECT 1 AS a) FORMAT TSVWithNames;

SELECT tuple(* APPLY (toTypeName, 'f_'), * APPLY (toString, 'f_')) FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT tuple(* APPLY (toTypeName, 'f_')), tuple(* APPLY (toString, 'f_')) FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * FROM (SELECT tuple(* APPLY (toTypeName, 'f_')), tuple(* APPLY (toString, 'f_')) FROM (SELECT 1 AS a)) FORMAT TSVWithNames;
SELECT tuple(toTypeName(a) AS f_a), tuple(toString(a) AS f_a) FROM (SELECT 1 AS a) FORMAT TSVWithNames;

SELECT * APPLY (x -> x, 'f_') FROM (SELECT 1 AS a) GROUP BY ALL WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1 FORMAT TSVWithNames;
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a) GROUP BY ALL SETTINGS group_by_use_nulls = 1 FORMAT TSVWithNames;
SELECT * APPLY (toTypeName, 'f_'), * APPLY (toString, 'f_') FROM (SELECT 1 AS a) GROUP BY ALL WITH ROLLUP SETTINGS group_by_use_nulls = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
-- The names of the discarded first resolution of GROUP BY ALL must not enable renaming, so this stays an error as without the prefix.
WITH 3 AS "1" SELECT 1, "1", * APPLY (x -> x, 'f_') FROM (SELECT 1 AS a) GROUP BY ALL WITH ROLLUP SETTINGS group_by_use_nulls = 1; -- { serverError AMBIGUOUS_COLUMN_NAME }

DROP TABLE t_05355;
