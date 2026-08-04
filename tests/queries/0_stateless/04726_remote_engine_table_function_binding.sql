-- The table-function target of the `Remote` / `RemoteSecure` engines is bound to the current database at
-- `CREATE` time, exactly like the `Distributed(cluster, table_function())` form: the persisted metadata must
-- not depend on the current database of whatever session queries the table later.

DROP TABLE IF EXISTS rtfb_src;
DROP TABLE IF EXISTS remote_merge;
DROP TABLE IF EXISTS remote_loop;
DROP TABLE IF EXISTS remote_declared;

CREATE TABLE rtfb_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO rtfb_src VALUES (1), (2), (3);

-- The single-argument `merge` would otherwise match tables of the querying session's current database.
CREATE TABLE remote_merge (x UInt64) ENGINE = Remote('127.0.0.1', merge('^rtfb_src$'));
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'remote_merge';
SELECT sum(x) FROM remote_merge;

-- The short form of `loop` would otherwise resolve its unqualified table name at query time.
CREATE TABLE remote_loop ENGINE = Remote('127.0.0.1', loop(rtfb_src));
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'remote_loop';
SELECT x FROM remote_loop LIMIT 4;

-- A `Remote` table over a table function carries its own declared structure, so the initiator must not
-- resolve the target locally: the target here is not resolvable on the initiator at all, and the only
-- shard is unavailable, so with `skip_unavailable_shards` the read returns no rows instead of failing.
CREATE TABLE remote_declared (x UInt64) ENGINE = Remote('127.0.0.1:1', merge(db_04726_does_not_exist, '^t$'));
SELECT * FROM remote_declared SETTINGS skip_unavailable_shards = 1, enable_analyzer = 1, send_logs_level = 'error';

DROP TABLE remote_declared;
DROP TABLE remote_loop;
DROP TABLE remote_merge;
DROP TABLE rtfb_src;
