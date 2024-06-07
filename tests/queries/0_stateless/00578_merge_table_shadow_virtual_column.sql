DROP TABLE IF EXISTS numbers1;
DROP TABLE IF EXISTS numbers2;

CREATE TABLE numbers1 ENGINE = Memory AS SELECT number as _table FROM numbers(1000);
CREATE TABLE numbers2 ENGINE = Memory AS SELECT number as _table FROM numbers(1000);

SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table='numbers1'; -- { serverError TYPE_MISMATCH }
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table=1;

DROP TABLE numbers1;
DROP TABLE numbers2;
