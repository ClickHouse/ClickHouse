-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;

-- A numeric column estimated against a string set element (from `IN (subquery)`, which does not
-- pre-coerce the literal) must not crash the countmin estimator with BAD_GET.
DROP TABLE IF EXISTS t_countmin_num;
CREATE TABLE t_countmin_num (id UInt64, value UInt64)
    ENGINE = MergeTree ORDER BY (id, value) SETTINGS auto_statistics_types = 'countmin';
INSERT INTO t_countmin_num SELECT number, number FROM numbers(10);

SELECT 'numeric column, string set element';
SELECT id FROM t_countmin_num WHERE id <= 10 AND value IN (SELECT '5') ORDER BY id;
SELECT 'numeric column, non-numeric string set element';
SELECT id FROM t_countmin_num WHERE id <= 10 AND value IN (SELECT 'abc') ORDER BY id;
SELECT 'numeric column, numeric set element';
SELECT id FROM t_countmin_num WHERE id <= 10 AND value IN (SELECT 5) ORDER BY id;

-- Mirror case: a string column estimated against a numeric set element.
DROP TABLE IF EXISTS t_countmin_str;
CREATE TABLE t_countmin_str (id UInt64, s String)
    ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'countmin';
INSERT INTO t_countmin_str SELECT number, toString(number) FROM numbers(10);

SELECT 'string column, numeric set element';
SELECT id FROM t_countmin_str WHERE id <= 10 AND s IN (SELECT 5) ORDER BY id;

DROP TABLE t_countmin_num;
DROP TABLE t_countmin_str;
