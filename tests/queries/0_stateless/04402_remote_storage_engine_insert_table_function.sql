-- Regression: a persistent `Remote`/`RemoteSecure` table whose target is a table function (e.g.
-- `numbers(...)`) can be read, but cannot be written to: there is no remote table to insert into,
-- so `remote_storage` is an empty `StorageID`. The write must be rejected with a clear error instead
-- of `DistributedSink` building an `INSERT` into an empty table id.

DROP TABLE IF EXISTS t_remote_tf;

-- The schema matches the `numbers` table function; the table-function target is read-only.
CREATE TABLE t_remote_tf (number UInt64) ENGINE = Remote('127.0.0.1', numbers(10));

-- Reading works.
SELECT count() FROM t_remote_tf;

-- Writing is rejected: the table-function target has no remote table to insert into.
INSERT INTO t_remote_tf VALUES (1); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_remote_tf;
