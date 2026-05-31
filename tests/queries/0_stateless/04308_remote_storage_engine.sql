-- Tags: shard

-- The `Remote` and `RemoteSecure` storage engines are the persistent counterparts of the
-- `remote` and `remoteSecure` table functions.

DROP TABLE IF EXISTS t_remote_engine;

-- Schema is inferred from the remote table when not specified explicitly.
CREATE TABLE t_remote_engine ENGINE = Remote('127.0.0.1', system, one);
SELECT * FROM t_remote_engine;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_remote_engine';
DROP TABLE t_remote_engine;

-- The `database.table` form is supported as well.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = Remote('127.0.0.1', 'system.one');
SELECT dummy FROM t_remote_engine;
DROP TABLE t_remote_engine;

-- An expression generating several shards is accepted, just like in the table function.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = Remote('127.0.0.{1,2}', system, one);
SELECT count() FROM t_remote_engine;
DROP TABLE t_remote_engine;

-- The user name can be passed explicitly.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = Remote('127.0.0.1', system, one, 'default');
SELECT dummy FROM t_remote_engine;
DROP TABLE t_remote_engine;

-- The `RemoteSecure` engine is registered and parses its arguments the same way.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = RemoteSecure('127.0.0.1', system, one);
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_remote_engine';
DROP TABLE t_remote_engine;

-- No arguments is an error.
CREATE TABLE t_remote_engine ENGINE = Remote; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
