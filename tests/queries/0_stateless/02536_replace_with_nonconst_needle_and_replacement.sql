-- Tests that functions replaceOne(), replaceAll(), replaceRegexpOne(), replaceRegexpAll() work with with non-const pattern and replacement arguments

DROP TABLE IF EXISTS test_tab;

CREATE TABLE test_tab
  (id UInt32, haystack String, needle String, replacement String)
  engine = MergeTree()
  ORDER BY id;

INSERT INTO test_tab VALUES (1, 'Hello World', 'l', 'xx') (2, 'Hello World', 'll', 'x') (3, 'Hello World', 'not_found', 'x') (4, 'Hello World', '[eo]', 'x') (5, 'Hello World', '.', 'x');


SELECT '** replaceAll() **';

SELECT '- non-const needle, const replacement';
SELECT id, haystack, needle, 'x', replaceAll(haystack, needle, 'x') FROM test_tab ORDER BY id;
SELECT id, haystack, needle, 'x', replaceAll('Hello World', needle, 'x') FROM test_tab ORDER BY id;

SELECT '- const needle, non-const replacement';
SELECT id, haystack, 'l', replacement, replaceAll(haystack, 'l', replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, 'l', replacement, replaceAll('Hello World', 'l', replacement) FROM test_tab ORDER BY id;

SELECT '- non-const needle, non-const replacement';
SELECT id, haystack, needle, replacement, replaceAll(haystack, needle, replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, needle, replacement, replaceAll('Hello World', needle, replacement) FROM test_tab ORDER BY id;


SELECT '** replaceOne() **';

SELECT '- non-const needle, const replacement';
SELECT id, haystack, needle, 'x', replaceOne(haystack, needle, 'x') FROM test_tab ORDER BY id;
SELECT id, haystack, needle, 'x', replaceOne('Hello World', needle, 'x') FROM test_tab ORDER BY id;

SELECT '- const needle, non-const replacement';
SELECT id, haystack, 'l', replacement, replaceOne(haystack, 'l', replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, 'l', replacement, replaceOne('Hello World', 'l', replacement) FROM test_tab ORDER BY id;

SELECT '- non-const needle, non-const replacement';
SELECT id, haystack, needle, replacement, replaceOne(haystack, needle, replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, needle, replacement, replaceOne('Hello World', needle, replacement) FROM test_tab ORDER BY id;

SELECT '** replaceRegexpAll() **';

SELECT '- non-const needle, const replacement';
SELECT id, haystack, needle, 'x', replaceRegexpAll(haystack, needle, 'x') FROM test_tab ORDER BY id;
SELECT id, haystack, needle, 'x', replaceRegexpAll('Hello World', needle, 'x') FROM test_tab ORDER BY id;

SELECT '- const needle, non-const replacement';
SELECT id, haystack, 'l', replacement, replaceRegexpAll(haystack, 'l', replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, 'l', replacement, replaceRegexpAll('Hello World', 'l', replacement) FROM test_tab ORDER BY id;

SELECT '- non-const needle, non-const replacement';
SELECT id, haystack, needle, replacement, replaceRegexpAll(haystack, needle, replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, needle, replacement, replaceRegexpAll('Hello World', needle, replacement) FROM test_tab ORDER BY id;

SELECT '** replaceRegexpOne() **';

SELECT '- non-const needle, const replacement';
SELECT id, haystack, needle, 'x', replaceRegexpOne(haystack, needle, 'x') FROM test_tab ORDER BY id;
SELECT id, haystack, needle, 'x', replaceRegexpOne('Hello World', needle, 'x') FROM test_tab ORDER BY id;

SELECT '- const needle, non-const replacement';
SELECT id, haystack, 'l', replacement, replaceRegexpOne(haystack, 'l', replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, 'l', replacement, replaceRegexpOne('Hello World', 'l', replacement) FROM test_tab ORDER BY id;

SELECT '- non-const needle, non-const replacement';
SELECT id, haystack, needle, replacement, replaceRegexpOne(haystack, needle, replacement) FROM test_tab ORDER BY id;
SELECT id, haystack, needle, replacement, replaceRegexpOne('Hello World', needle, replacement) FROM test_tab ORDER BY id;

DROP TABLE IF EXISTS test_tab;

SELECT 'Empty needles do not throw an exception';

CREATE TABLE test_tab
  (id UInt32, haystack String, needle String, replacement String)
  engine = MergeTree()
  ORDER BY id;

INSERT INTO test_tab VALUES (1, 'Hello World', 'l', 'x') (2, 'Hello World', '', 'y');

SELECT '- non-const needle, const replacement';
SELECT replaceAll(haystack, needle, 'x') FROM test_tab;
SELECT replaceOne(haystack, needle, 'x') FROM test_tab;
SELECT replaceRegexpAll(haystack, needle, 'x') FROM test_tab;
SELECT replaceRegexpOne(haystack, needle, 'x') FROM test_tab;

SELECT '- const needle, non-const replacement';
SELECT replaceAll(haystack, '', replacement) FROM test_tab;
SELECT replaceOne(haystack, '', replacement) FROM test_tab;
SELECT replaceRegexpAll(haystack, '', replacement) FROM test_tab;
SELECT replaceRegexpOne(haystack, '', replacement) FROM test_tab;

SELECT '- non-const needle, non-const replacement';
SELECT replaceAll(haystack, needle, replacement) FROM test_tab;
SELECT replaceOne(haystack, needle, replacement) FROM test_tab;
SELECT replaceRegexpAll(haystack, needle, replacement) FROM test_tab;
SELECT replaceRegexpOne(haystack, needle, replacement) FROM test_tab;

DROP TABLE IF EXISTS test_tab;
