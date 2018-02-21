DROP TABLE IF EXISTS test.numbers1;
DROP TABLE IF EXISTS test.numbers2;
DROP TABLE IF EXISTS test.numbers3;
DROP TABLE IF EXISTS test.numbers4;
DROP TABLE IF EXISTS test.numbers5;

CREATE TABLE test.numbers1 ENGINE = StripeLog AS SELECT number FROM numbers(1000);
CREATE TABLE test.numbers2 ENGINE = TinyLog AS SELECT number FROM numbers(1000);
CREATE TABLE test.numbers3 ENGINE = Log AS SELECT number FROM numbers(1000);
CREATE TABLE test.numbers4 ENGINE = Memory AS SELECT number FROM numbers(1000);
CREATE TABLE test.numbers5 ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(1000);

SELECT count() FROM merge(test, '^numbers\\d+$');
SELECT DISTINCT count() FROM merge(test, '^numbers\\d+$') GROUP BY number;

SET max_rows_to_read = 1000;

SET max_threads = 'auto';
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table = 'numbers1';

SET max_threads = 1;
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table = 'numbers2';

SET max_threads = 10;
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table = 'numbers3';

SET max_rows_to_read = 1;

SET max_threads = 'auto';
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table = 'non_existing';

SET max_threads = 1;
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table = 'non_existing';

SET max_threads = 10;
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table = 'non_existing';

DROP TABLE test.numbers1;
DROP TABLE test.numbers2;
DROP TABLE test.numbers3;
DROP TABLE test.numbers4;
DROP TABLE test.numbers5;
