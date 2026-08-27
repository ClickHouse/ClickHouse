-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

-- Reading the destination table of a `Buffer` table can return a full column where the plan over
-- the buffers keeps it constant: constants come back materialized from a `Distributed` destination.
-- A full column cannot be converted back to a constant, so `StorageBuffer` must materialize such
-- constants in the buffers branch instead of failing with `ILLEGAL_COLUMN`.

DROP TABLE IF EXISTS 04817_dest;
DROP TABLE IF EXISTS 04817_dist;
DROP TABLE IF EXISTS 04817_m_buffer;
DROP TABLE IF EXISTS 04817_m_rocksdb;

CREATE TABLE 04817_dest (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE 04817_dist (k UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), '04817_dest');
CREATE TABLE 04817_m_buffer (k UInt64) ENGINE = Buffer(currentDatabase(), '04817_dist', 1, 1, 10, 10000, 1000000, 10000000, 100000000);
CREATE TABLE 04817_m_rocksdb (k UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (k);

INSERT INTO 04817_dest VALUES (1), (2);
INSERT INTO 04817_m_rocksdb VALUES (3), (4);

SELECT DISTINCT 42 FROM 04817_m_buffer QUALIFY materialize(42);

-- The same `Buffer` under a `Merge` table next to a non-distributed sibling: the `Buffer` child
-- exposes materialized constants while the `EmbeddedRocksDB` child keeps them constant, and the
-- headers of the children must still agree when the children are united.
SELECT DISTINCT 42 FROM merge(currentDatabase(), '^04817_m_') QUALIFY materialize(42);
SELECT DISTINCT 42 FROM merge(currentDatabase(), '^04817_m_') QUALIFY materialize(42) SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

DROP TABLE 04817_m_buffer;
DROP TABLE 04817_dist;
DROP TABLE 04817_dest;
DROP TABLE 04817_m_rocksdb;
