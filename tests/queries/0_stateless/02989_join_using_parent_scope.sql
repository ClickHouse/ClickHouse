DROP TABLE IF EXISTS tabc;
CREATE TABLE tabc (a UInt32, b UInt32 ALIAS a + 1, c UInt32 ALIAS b + 1, s String) ENGINE = MergeTree ORDER BY a;
INSERT INTO tabc (a, s) SELECT number, 'abc' || toString(number) FROM numbers(4);

DROP TABLE IF EXISTS ta;
CREATE TABLE ta (a Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ta SELECT number FROM numbers(4);

DROP TABLE IF EXISTS tb;
CREATE TABLE tb (b Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tb SELECT number FROM numbers(4);

SET join_use_nulls = 1;

SET analyzer_compatibility_join_using_top_level_identifier = 1;

-- { echoOn }
SELECT 1 AS c0 FROM (SELECT 1 AS c1) t0 JOIN (SELECT 1 AS c0) t1 USING (c0);
SELECT 1 AS c0 FROM (SELECT 1 AS c0) t0 JOIN (SELECT 1 AS c0) t1 USING (c0);

SELECT 1 AS a FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT a + 2 AS b FROM ta JOIN tabc USING (b) ORDER BY ALL;
SELECT b + 2 AS a FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT a + 2 AS c FROM ta JOIN tabc USING (c) ORDER BY ALL;

SELECT b AS a, a FROM tb JOIN tabc USING (a) ORDER BY ALL;
SELECT 1 AS b FROM tb JOIN ta USING (b); -- { serverError UNKNOWN_IDENTIFIER }

-- SELECT * returns all columns from both tables in new analyzer
SELECT 3 AS a, a, * FROM tb FULL JOIN tabc USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM tb JOIN tabc USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;

SELECT b + 1 AS a, * FROM tb JOIN tabc USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM tb LEFT JOIN tabc USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM tb RIGHT JOIN tabc USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM tb FULL JOIN tabc USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM tb FULL JOIN tabc USING (a) ORDER BY ALL SETTINGS asterisk_include_alias_columns = 1, enable_analyzer = 1;

SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 LEFT JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 RIGHT JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;
SELECT b + 1 AS a, * FROM (SELECT b FROM tb) t1 FULL JOIN (SELECT a, b FROM tabc) t2 USING (a) ORDER BY ALL SETTINGS enable_analyzer = 1;

SELECT b + 1 AS a, s FROM tb FULL OUTER JOIN tabc USING (a) PREWHERE a > 2 ORDER BY ALL SETTINGS enable_analyzer = 1;

EXPLAIN PIPELINE SELECT (SELECT 1) AS c0 FROM (SELECT 1 AS c0, 1 AS c1) tx JOIN (SELECT 0 AS c0, 1 AS c1) ty USING (c0, c1) FORMAT Null SETTINGS enable_analyzer = 1;

-- It's a default behavior for old analyzer and new with analyzer_compatibility_join_using_top_level_identifier
-- Column `b` actually exists in left table, but `b` from USING is resoled to `a + 2` and `a` is not in left table
-- so we get UNKNOWN_IDENTIFIER error.
SELECT a + 2 AS b FROM tb JOIN tabc USING (b) ORDER BY ALL
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError UNKNOWN_IDENTIFIER }

-- In new analyzer with `analyzer_compatibility_join_using_top_level_identifier = 0` we get `b` from left table
SELECT a + 2 AS b FROM tb JOIN tabc USING (b) ORDER BY ALL
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 0, enable_analyzer = 1;

-- This is example where query may return different results with different `analyzer_compatibility_join_using_top_level_identifier`

DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, spouse_name String) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 'Ksenia');
INSERT INTO users VALUES (6666, 'Ksenia', '');

SELECT u1.uid, u1.spouse_name as name, u2.uid, u2.name
FROM users u1 JOIN users u2 USING (name)
ORDER BY u1.uid
FORMAT TSVWithNamesAndTypes
SETTINGS enable_analyzer = 1, analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT u1.uid, u1.spouse_name as name, u2.uid, u2.name
FROM users u1 JOIN users u2 USING (name)
ORDER BY u1.uid
FORMAT TSVWithNamesAndTypes
SETTINGS enable_analyzer = 1, analyzer_compatibility_join_using_top_level_identifier = 0;

SELECT u1.uid, u1.spouse_name as name, u2.uid, u2.name
FROM users u1 JOIN users u2 USING (name)
ORDER BY u1.uid
FORMAT TSVWithNamesAndTypes
SETTINGS enable_analyzer = 0;

DROP TABLE IF EXISTS users;


DROP TABLE IF EXISTS tabc;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS tc;
