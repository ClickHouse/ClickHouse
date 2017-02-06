DROP TABLE IF EXISTS test.stripe1;
DROP TABLE IF EXISTS test.stripe2;
DROP TABLE IF EXISTS test.stripe3;
DROP TABLE IF EXISTS test.stripe4;
DROP TABLE IF EXISTS test.stripe5;
DROP TABLE IF EXISTS test.stripe6;
DROP TABLE IF EXISTS test.stripe7;
DROP TABLE IF EXISTS test.stripe8;
DROP TABLE IF EXISTS test.stripe9;
DROP TABLE IF EXISTS test.stripe10;
DROP TABLE IF EXISTS test.merge;

CREATE TABLE test.stripe1 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe2 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe3 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe4 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe5 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe6 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe7 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe8 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe9 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
CREATE TABLE test.stripe10 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;

CREATE TABLE test.merge AS test.stripe1 ENGINE = Merge(test, '^stripe\\d+');

SELECT x, count() FROM test.merge GROUP BY x ORDER BY x;
SET max_threads = 1;
SELECT x, count() FROM test.merge GROUP BY x ORDER BY x;
SET max_threads = 2;
SELECT x, count() FROM test.merge GROUP BY x ORDER BY x;
SET max_threads = 5;
SELECT x, count() FROM test.merge GROUP BY x ORDER BY x;
SET max_threads = 10;
SELECT x, count() FROM test.merge GROUP BY x ORDER BY x;
SET max_threads = 20;
SELECT x, count() FROM test.merge GROUP BY x ORDER BY x;

DROP TABLE IF EXISTS test.stripe1;
DROP TABLE IF EXISTS test.stripe2;
DROP TABLE IF EXISTS test.stripe3;
DROP TABLE IF EXISTS test.stripe4;
DROP TABLE IF EXISTS test.stripe5;
DROP TABLE IF EXISTS test.stripe6;
DROP TABLE IF EXISTS test.stripe7;
DROP TABLE IF EXISTS test.stripe8;
DROP TABLE IF EXISTS test.stripe9;
DROP TABLE IF EXISTS test.stripe10;
DROP TABLE IF EXISTS test.merge;
