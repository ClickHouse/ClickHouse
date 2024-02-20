DROP TABLE IF EXISTS tabc;
CREATE TABLE tabc (a UInt32, b UInt32 ALIAS a + 1, c UInt32 ALIAS b + 1) ENGINE = MergeTree ORDER BY a;
INSERT INTO tabc SELECT number FROM numbers(4);

DROP TABLE IF EXISTS ta;
CREATE TABLE ta (a Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ta SELECT number FROM numbers(4);

DROP TABLE IF EXISTS tb;
CREATE TABLE tb (b Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tb SELECT number FROM numbers(4);

SET join_use_nulls = 1;

-- { echoOn }
SELECT 1 AS a FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT a + 2 AS b FROM ta JOIN tabc USING (b) ORDER BY ALL;
SELECT b + 2 AS a FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT a + 2 AS c FROM ta JOIN tabc USING (c) ORDER BY ALL;

SELECT b AS a, a FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT 1 AS b FROM tb JOIN ta USING (b); -- { serverError UNKNOWN_IDENTIFIER }

-- SELECT * works returns all columns from both tables in new analyzer
SET allow_experimental_analyzer = 1;

SELECT 3 AS a, a, * FROM tb FULL JOIN tabc USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM tb JOIN tabc USING (a) ORDER BY ALL;

SELECT b + 1 AS a, * FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM tb LEFT JOIN tabc USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM tb RIGHT JOIN tabc USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM tb FULL JOIN tabc USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM tb FULL JOIN tabc USING (a) ORDER BY ALL SETTINGS asterisk_include_alias_columns = 1;

SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 LEFT JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 RIGHT JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL;
SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 FULL JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL;


DROP TABLE IF EXISTS tabc;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS tc;
