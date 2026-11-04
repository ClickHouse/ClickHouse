-- A column named `from` is an unquoted keyword identifier, which is allowed. Inside parentheses it is
-- ambiguous with a subquery in the FROM-first form (`(FROM t)` means `(SELECT * FROM t)`); the column
-- reading wins.

DROP TABLE IF EXISTS t_column_named_from;
CREATE TABLE t_column_named_from (`from` Nullable(String), c1 UInt8) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_column_named_from VALUES ('a', 1), ('b', 2), (NULL, 3);

SELECT count() FROM t_column_named_from WHERE (from IN ('a'));
SELECT count() FROM t_column_named_from WHERE (from NOT IN ('a'));
SELECT count() FROM t_column_named_from WHERE (from IS NULL);
SELECT count() FROM t_column_named_from WHERE (from IS NOT NULL);
SELECT count() FROM t_column_named_from WHERE (from LIKE 'a%');
SELECT count() FROM t_column_named_from WHERE (from = 'a');
SELECT count() FROM t_column_named_from WHERE (from BETWEEN 'a' AND 'b');
SELECT count() FROM t_column_named_from WHERE ((from IS NOT NULL) AND (from != 'b'));
SELECT count() FROM t_column_named_from WHERE (from IS NULL) OR (from = 'a');
SELECT count() FROM t_column_named_from WHERE ((((from IN ('a')))));
SELECT c1, from FROM t_column_named_from WHERE (from IS NOT NULL) ORDER BY c1;

DROP TABLE t_column_named_from;

-- The same ambiguity without a table.
SELECT (from IN (1)) FROM (SELECT 1 AS `from`);
SELECT (from IS NULL) FROM (SELECT 1 AS `from`);
SELECT (from IS NOT NULL) FROM (SELECT 1 AS `from`);
SELECT (from BETWEEN 0 AND 2) FROM (SELECT 1 AS `from`);
SELECT (from AND 1) FROM (SELECT 1 AS `from`);
SELECT (from OR 0) FROM (SELECT 1 AS `from`);
SELECT (from IS NULL ? 2 : 3) FROM (SELECT 1 AS `from`);
SELECT (from + 1) FROM (SELECT 1 AS `from`);
SELECT (from::String) FROM (SELECT 1 AS `from`);
SELECT (from.1) FROM (SELECT (1, 2) AS `from`);
SELECT (from[1]) FROM (SELECT [1, 2] AS `from`);
SELECT (from IN (1), from + 1) FROM (SELECT 1 AS `from`);
SELECT (from IN (1) AS in_a) FROM (SELECT 1 AS `from`);
SELECT formatQuery('SELECT count() FROM t WHERE (from IN (1))');

-- A subquery in the FROM-first form keeps its reading in the same positions.
SELECT 1 IN (FROM system.one);
SELECT * FROM (FROM system.one);
SELECT 1 IN (FROM (SELECT 1));
SELECT (FROM numbers(3) |> SELECT count());
SELECT (FROM system.one |> SELECT 1);

-- Nested parentheses over a column named `from` must not take exponential time to parse.
SELECT (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + 1)))))))))))))))))))))))))))))))))))))))) FROM (SELECT 1 AS `from`);
SELECT (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND (from AND 1)))))))))))))))))))))))))))))))))))))))) FROM (SELECT 1 AS `from`);
SELECT (from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN ((from IN (1)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) FROM (SELECT 1 AS `from`);

-- The lookahead's backtracking counts against `max_parser_backtracks`.
SELECT formatQuery('SELECT (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + 1))))))))))))))))))))))))))))))))))))))))') SETTINGS max_parser_backtracks = 1300; -- { serverError TOO_SLOW_PARSING }
SELECT length(formatQuery('SELECT (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + (from + 1))))))))))))))))))))))))))))))))))))))))')) > 0 SETTINGS max_parser_backtracks = 24000;
