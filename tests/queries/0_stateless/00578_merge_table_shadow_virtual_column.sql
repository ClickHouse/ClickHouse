DROP TABLE IF EXISTS test.numbers1;
DROP TABLE IF EXISTS test.numbers2;

CREATE TABLE test.numbers1 ENGINE = Memory AS SELECT number as _table FROM numbers(1000);
CREATE TABLE test.numbers2 ENGINE = Memory AS SELECT number as _table FROM numbers(1000);

SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table='numbers1'; -- { serverError 43 }
SELECT count() FROM merge(test, '^numbers\\d+$') WHERE _table=1;

DROP TABLE test.numbers1;
DROP TABLE test.numbers2;
