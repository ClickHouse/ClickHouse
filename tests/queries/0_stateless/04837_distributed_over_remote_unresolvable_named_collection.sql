-- Tags: no-parallel
-- no-parallel: CREATE/DROP NAMED COLLECTION mutate global server state shared by concurrent tests
-- (see 02918_fuzzjson_table_function.sql for the same requirement), and the flaky check runs this very
-- test concurrently with itself, so one run's DROP NAMED COLLECTION would remove the collection from
-- under another run.

-- A persisted `remote(named_collection)` target reads its database from the collection, and an empty
-- value stored there resolves against the current database of the session that reads the table
-- (04826_distributed_over_remote_named_collection_internal_database), so the collection is resolved at
-- CREATE time and an empty stored database is frozen. A collection that this server does not have -
-- for example one defined only on the shards of a cluster with no local replica, where nothing on the
-- initiator ever parses the inner function - cannot be resolved here at all, so its stored database can
-- neither be read nor frozen and the definition would stay session-dependent. Such a target is rejected
-- instead of being persisted unbound.
DROP NAMED COLLECTION IF EXISTS nc_04837_local;
CREATE NAMED COLLECTION nc_04837_local AS addresses_expr = '127.0.0.1', database = '', table = 'bind_src';

-- The `key = value` form leaves no other reading of the first argument: it must name a collection.
CREATE TABLE dist_nc_unknown (n UInt64)
    ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, remote(nc_04837_does_not_exist, table = 'bind_src')); -- { serverError BAD_ARGUMENTS }

-- The single-argument form as well, when the name is neither a collection nor a configured cluster.
CREATE TABLE dist_nc_unknown_single (n UInt64)
    ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, remote(nc_04837_does_not_exist)); -- { serverError BAD_ARGUMENTS }

-- Naming the database explicitly makes the target well defined without resolving the collection, so it
-- is accepted (and the literal override is persisted as is).
CREATE TABLE dist_nc_unknown_with_database (n UInt64)
    ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, remote(nc_04837_does_not_exist, database = 'bind_db', table = 'bind_src'));
SHOW CREATE TABLE dist_nc_unknown_with_database;
-- A `db = ...` override names the same key.
CREATE TABLE dist_nc_unknown_with_db (n UInt64)
    ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, remote(nc_04837_does_not_exist, db = 'bind_db', table = 'bind_src'));
SHOW CREATE TABLE dist_nc_unknown_with_db;
-- Loading such a definition back from the metadata does not re-check anything.
DETACH TABLE dist_nc_unknown_with_database;
ATTACH TABLE dist_nc_unknown_with_database;
SHOW CREATE TABLE dist_nc_unknown_with_database;

-- A single identifier that names a configured cluster is not the named-collection form: the target is
-- then the fixed `system.one` placeholder, which does not depend on the current database.
CREATE TABLE dist_cluster_name (dummy UInt8)
    ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, remote(test_shard_localhost));
SHOW CREATE TABLE dist_cluster_name;

-- A collection this server does have is still resolved and its empty stored database still frozen.
CREATE TABLE bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_src VALUES (1), (2), (3);
CREATE TABLE dist_nc_local ENGINE = Distributed(test_shard_localhost, remote(nc_04837_local));
SHOW CREATE TABLE dist_nc_local;
SELECT sum(n) FROM dist_nc_local;

DROP TABLE dist_nc_local;
DROP TABLE bind_src;
DROP TABLE dist_cluster_name;
DROP TABLE dist_nc_unknown_with_db;
DROP TABLE dist_nc_unknown_with_database;
DROP NAMED COLLECTION nc_04837_local;
