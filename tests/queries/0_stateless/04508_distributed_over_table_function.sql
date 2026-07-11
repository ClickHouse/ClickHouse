-- A Distributed table can be created over a table function, like the `cluster`/`remote` table functions.

DROP TABLE IF EXISTS dist_over_tf;
DROP TABLE IF EXISTS dist_over_tf_local;

-- The structure is inferred from the table function; single (local) shard.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT sum(number), count() FROM dist_over_tf;
-- The table function survives a metadata round-trip (it is re-parsed on ATTACH).
DETACH TABLE dist_over_tf;
ATTACH TABLE dist_over_tf;
SELECT sum(number), count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- Two shards: the table function is executed on every shard.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10));
SELECT count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- Explicit columns and an optional sharding key are accepted.
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), number);
SELECT count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- A second argument that is not a registered table function is still treated as a database name,
-- so the classic `Distributed(cluster, database, table)` form (including `currentDatabase()`) is unaffected.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
INSERT INTO dist_over_tf_local VALUES (1), (2), (3);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
SELECT sum(x) FROM dist_over_tf;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;

-- INSERT into a table-function-backed Distributed table is rejected (there is no concrete remote table).
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf VALUES (100); -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- Too many arguments for the table-function form.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10), number, 'default', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
