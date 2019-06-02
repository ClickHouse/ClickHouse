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

CREATE TABLE merge AS stripe1 ENGINE = Merge(default, '^stripe\\d+');

SELECT x, count() FROM merge GROUP BY x ORDER BY x;
SET max_threads = 1;
SELECT x, count() FROM merge GROUP BY x ORDER BY x;
SET max_threads = 2;
SELECT x, count() FROM merge GROUP BY x ORDER BY x;
SET max_threads = 5;
SELECT x, count() FROM merge GROUP BY x ORDER BY x;
SET max_threads = 10;
SELECT x, count() FROM merge GROUP BY x ORDER BY x;
SET max_threads = 20;
SELECT x, count() FROM merge GROUP BY x ORDER BY x;
