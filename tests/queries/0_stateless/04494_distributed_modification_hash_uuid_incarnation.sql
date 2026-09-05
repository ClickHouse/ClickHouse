-- Tests that the modification_hash of a Distributed table folds the table's own UUID, so that a same-name
-- DROP + CREATE (a new incarnation) is not mistaken for the same data state (issue #108713).
--
-- StorageDistributed::getModificationHash requires the table to have a UUID (hasUUID()) precisely so that
-- incarnations of the same Distributed name can be told apart: the create-time read-affecting arguments (the
-- sharding key, the cluster/remote target) are fixed at creation and cannot be folded as in-lifetime changes.
-- The UUID must therefore be mixed into the hash. Otherwise, in an Atomic database, a query cached for
-- ENGINE = Distributed(..., x) could survive recreating the same name as ENGINE = Distributed(..., x + 1)
-- over the same remote tables, because every other folded value (columns, remote db/table, per-shard hashes)
-- stays identical while optimize_skip_unused_shards would now route the query to a different shard.

DROP TABLE IF EXISTS dist_uuid_src;
DROP TABLE IF EXISTS dist_uuid;
DROP TABLE IF EXISTS dist_uuid_saved_hash;

CREATE TABLE dist_uuid_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dist_uuid_src VALUES (1);

-- test_shard_localhost resolves to the local shard, which recurses into dist_uuid_src's hash in-process. The
-- default (Atomic) database gives the Distributed table a UUID, so getModificationHash does not fail closed.
CREATE TABLE dist_uuid (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_uuid_src, x);

SELECT 'hash not null', modification_hash IS NOT NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_uuid';

-- Remember the first incarnation's hash.
CREATE TABLE dist_uuid_saved_hash (h Nullable(UInt128)) ENGINE = Memory;
INSERT INTO dist_uuid_saved_hash SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'dist_uuid';

-- Recreate the same name with everything identical. In an Atomic database this assigns a fresh UUID, so the
-- hash must differ solely because the table's own UUID is folded. Before the fix the UUID was required by the
-- hasUUID() guard but never mixed in, so the two incarnations collided.
DROP TABLE dist_uuid;
CREATE TABLE dist_uuid (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_uuid_src, x);
SELECT 'identical recreate hash differs', (SELECT h FROM dist_uuid_saved_hash) != (SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'dist_uuid');

-- A create-time sharding-key change (query-visible under optimize_skip_unused_shards) must not be mistaken for
-- the same data state either.
DROP TABLE dist_uuid;
CREATE TABLE dist_uuid (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_uuid_src, x + 1);
SELECT 'sharding key change hash differs', (SELECT h FROM dist_uuid_saved_hash) != (SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'dist_uuid');

DROP TABLE dist_uuid;
DROP TABLE dist_uuid_src;
DROP TABLE dist_uuid_saved_hash;
