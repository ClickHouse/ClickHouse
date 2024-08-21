DROP TABLE IF EXISTS numbers1;
DROP TABLE IF EXISTS numbers2;
DROP TABLE IF EXISTS numbers3;
DROP TABLE IF EXISTS numbers4;
DROP TABLE IF EXISTS numbers5;

CREATE TABLE numbers1 ENGINE = StripeLog AS SELECT number FROM numbers(1000);
CREATE TABLE numbers2 ENGINE = TinyLog AS SELECT number FROM numbers(1000);
CREATE TABLE numbers3 ENGINE = Log AS SELECT number FROM numbers(1000);
CREATE TABLE numbers4 ENGINE = Memory AS SELECT number FROM numbers(1000);
CREATE TABLE numbers5 ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(1000);

SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$');
SELECT DISTINCT count() FROM merge(currentDatabase(), '^numbers\\d+$') GROUP BY number;

SET max_rows_to_read = 1000;

SET max_threads = 'auto';
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'numbers1';

SET max_threads = 1;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'numbers2';

SET max_threads = 10;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'numbers3';

SET max_rows_to_read = 1;

SET max_threads = 'auto';
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'non_existing';

SET max_threads = 1;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'non_existing';

SET max_threads = 10;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'non_existing';

DROP TABLE numbers1;
DROP TABLE numbers2;
DROP TABLE numbers3;
DROP TABLE numbers4;
DROP TABLE numbers5;
