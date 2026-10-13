-- A table function that has not been resolved yet reports no row count. `DROP` and `DETACH ... IF EMPTY`
-- only remove the table from the catalog, so they go ahead without resolving it. `TRUNCATE ... IF EMPTY`
-- would resolve it and truncate the storage behind it, so it refuses.
-- ignore_drop_queries_probability = 0: the stress runner sets it, and a rewritten `DROP` becomes a
-- `TRUNCATE` that resolves the table function.
SET ignore_drop_queries_probability = 0;

CREATE TABLE mem (x UInt64) ENGINE = Memory;
INSERT INTO mem VALUES (1), (2), (3);

CREATE TABLE tf_truncate (x UInt64) AS merge(currentDatabase(), '^mem$');
TRUNCATE TABLE IF EMPTY tf_truncate; -- { serverError TABLE_NOT_EMPTY }
SELECT count() FROM mem;

CREATE TABLE tf_drop (x UInt64) AS merge(currentDatabase(), '^mem$');
DROP TABLE IF EMPTY tf_drop;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tf_drop';

CREATE TABLE tf_detach (x UInt64) AS merge(currentDatabase(), '^mem$');
DETACH TABLE IF EMPTY tf_detach;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tf_detach';
ATTACH TABLE tf_detach;
SELECT count() FROM tf_detach;

SELECT count() FROM mem;
