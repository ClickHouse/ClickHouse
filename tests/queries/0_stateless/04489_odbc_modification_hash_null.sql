-- Tags: no-fasttest
-- no-fasttest: the ODBC engine and the odbc-bridge are not available in the fast-test build.

-- ODBC / JDBC tables (StorageXDBC) extend the URL storage but read through the bridge with a
-- query-specific POST, so the URL `ETag` probe inherited from `IStorageURLBase` is meaningless for them.
-- They must report a NULL modification_hash so the consistency consumers (the query cache and
-- `REFRESH ... IF CHANGED`) fail closed rather than reuse a result validated against an unrelated bridge
-- endpoint. Creating the table is lazy (it does not contact the bridge), and reading modification_hash
-- short-circuits to NULL, so this needs no running bridge.

DROP TABLE IF EXISTS t_odbc_modhash;
CREATE TABLE t_odbc_modhash (x UInt64) ENGINE = ODBC('DSN=fake;Database=fake', somedb, sometable);
SELECT 'odbc modification_hash null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_odbc_modhash';
DROP TABLE t_odbc_modhash;
