CREATE TABLE numbers1 ENGINE = Memory AS SELECT number as _table FROM numbers(1000);
CREATE TABLE numbers2 ENGINE = Memory AS SELECT number as _table FROM numbers(1000);

SELECT count() FROM merge(default, '^numbers\\d+$') WHERE _table='numbers1'; -- { serverError 43 }
SELECT count() FROM merge(default, '^numbers\\d+$') WHERE _table=1;
