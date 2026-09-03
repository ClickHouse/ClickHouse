DROP TABLE IF EXISTS stripe1;
DROP TABLE IF EXISTS stripe2;
DROP TABLE IF EXISTS stripe3;
DROP TABLE IF EXISTS stripe4;
DROP TABLE IF EXISTS stripe5;
DROP TABLE IF EXISTS stripe6;
DROP TABLE IF EXISTS stripe7;
DROP TABLE IF EXISTS stripe8;
DROP TABLE IF EXISTS stripe9;
DROP TABLE IF EXISTS stripe10;
DROP TABLE IF EXISTS merge_00401;

CREATE TABLE stripe1 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe2 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe3 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe4 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe5 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe6 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe7 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe8 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe9 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE stripe10 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;

CREATE TABLE merge_00401 AS stripe1 ENGINE = Merge(currentDatabase(), '^stripe\\d+');

SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 1;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 2;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 5;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 10;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 20;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;

DROP TABLE IF EXISTS stripe1;
DROP TABLE IF EXISTS stripe2;
DROP TABLE IF EXISTS stripe3;
DROP TABLE IF EXISTS stripe4;
DROP TABLE IF EXISTS stripe5;
DROP TABLE IF EXISTS stripe6;
DROP TABLE IF EXISTS stripe7;
DROP TABLE IF EXISTS stripe8;
DROP TABLE IF EXISTS stripe9;
DROP TABLE IF EXISTS stripe10;
DROP TABLE IF EXISTS merge_00401;
