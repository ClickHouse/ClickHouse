-- With an explicit column list, the create-time analysis of a table-function target runs purely for its
-- access side effect, so a target that is not resolvable right now must not fail the `CREATE`: it is
-- deferred and validated again when a query reaches the shard. This must hold for the `Remote` engine
-- exactly as it does for `Distributed(cluster, table_function())` - the `Remote` branch used to tolerate
-- the absent target only on a backup `RESTORE` and rejected the same definition on a plain `CREATE`.

DROP TABLE IF EXISTS remote_absent_tf;
DROP TABLE IF EXISTS dist_absent_tf;
DROP TABLE IF EXISTS absent_tf_src;

-- The target matches nothing yet: the `CREATE` succeeds thanks to the explicit columns, the read is
-- rejected at query time, and once the backing table appears the same table becomes readable.
CREATE TABLE remote_absent_tf (n UInt64) ENGINE = Remote('127.0.0.1', merge(currentDatabase(), '^absent_tf_src$'));
SELECT * FROM remote_absent_tf; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
CREATE TABLE absent_tf_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO absent_tf_src VALUES (1), (2), (3);
SELECT sum(n) FROM remote_absent_tf;
DROP TABLE absent_tf_src;

-- The `Distributed` sibling keeps behaving the same way over a cluster with a local shard.
CREATE TABLE dist_absent_tf (n UInt64) ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), '^absent_tf_src$'));
SELECT * FROM dist_absent_tf; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
CREATE TABLE absent_tf_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO absent_tf_src VALUES (4), (5), (6);
SELECT sum(n) FROM dist_absent_tf;

-- Without an explicit column list the analysis is the only source of the table's structure, so an
-- unresolvable target still fails the `CREATE` for both engines.
DROP TABLE absent_tf_src;
CREATE TABLE remote_absent_tf_infer ENGINE = Remote('127.0.0.1', merge(currentDatabase(), '^absent_tf_src$')); -- { serverError UNKNOWN_TABLE }
CREATE TABLE dist_absent_tf_infer ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), '^absent_tf_src$')); -- { serverError UNKNOWN_TABLE }

DROP TABLE remote_absent_tf;
DROP TABLE dist_absent_tf;
