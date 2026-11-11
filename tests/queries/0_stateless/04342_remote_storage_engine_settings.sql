-- The `Remote` and `RemoteSecure` storage engines must validate `DistributedSettings` and expose
-- source-level documentation, exactly like the `Distributed` engine they build upon.

-- The engines are documented: `system.table_engines` reports a non-empty description and syntax.
SELECT name, description != '' AS has_description, syntax != '' AS has_syntax
FROM system.table_engines
WHERE name IN ('Remote', 'RemoteSecure')
ORDER BY name;

DROP TABLE IF EXISTS t_remote_engine;

-- `max_delay_to_insert` must be at least 1, just like for `ENGINE = Distributed`.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = Remote('127.0.0.1', system, one)
SETTINGS max_delay_to_insert = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- `bytes_to_throw_insert` is handled before `bytes_to_delay_insert`, so it cannot be less or equal.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = Remote('127.0.0.1', system, one)
SETTINGS bytes_to_throw_insert = 1, bytes_to_delay_insert = 2; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The same validation applies to `RemoteSecure`.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = RemoteSecure('127.0.0.1', system, one)
SETTINGS max_delay_to_insert = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A valid combination of settings is accepted.
CREATE TABLE t_remote_engine (dummy UInt8) ENGINE = Remote('127.0.0.1', system, one)
SETTINGS max_delay_to_insert = 5, bytes_to_throw_insert = 100, bytes_to_delay_insert = 50;
SELECT dummy FROM t_remote_engine;
DROP TABLE t_remote_engine;
