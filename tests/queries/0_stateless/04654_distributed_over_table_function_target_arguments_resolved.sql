-- Analyzing the table-function target of a `Distributed` table at `CREATE` resolves the target's arguments to
-- literals in the stored definition. This is intentional and load-bearing: the stored definition is formatted
-- and sent to the other shards, so an argument that depends on the session - such as `currentDatabase()` - has
-- to be resolved on the initiator, otherwise a shard would resolve it against its own default database.

DROP TABLE IF EXISTS res_src;
DROP TABLE IF EXISTS dist_inferred;
DROP TABLE IF EXISTS dist_explicit;
DROP TABLE IF EXISTS dist_two_shards;

CREATE TABLE res_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO res_src VALUES (1), (2), (3);

-- The column list is omitted, so the target is analyzed at `CREATE` to infer the structure.
CREATE TABLE dist_inferred ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), concat('^res', '_src$')));
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_inferred';
SELECT sum(x) FROM dist_inferred;

-- With an explicit column list the target is still analyzed, to check the creator's access to it.
CREATE TABLE dist_explicit (x UInt64) ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), concat('^res', '_src$')));
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'dist_explicit';
SELECT sum(x) FROM dist_explicit SETTINGS prefer_localhost_replica = 1;

-- The resolved definition is what reaches the other shards.
CREATE TABLE dist_two_shards ENGINE = Distributed(test_cluster_two_shards, merge(currentDatabase(), '^res_src$'));
SELECT sum(x) FROM dist_two_shards SETTINGS prefer_localhost_replica = 0;

DROP TABLE dist_two_shards;
DROP TABLE dist_explicit;
DROP TABLE dist_inferred;
DROP TABLE res_src;
