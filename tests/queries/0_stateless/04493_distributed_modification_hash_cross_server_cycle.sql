-- Tests that the modification_hash of a Distributed table fails closed on a cross-server cycle (issue #108713).
--
-- The same-server cycle guard in StorageDistributed::getModificationHash is a thread-local set, so it only
-- catches recursion that stays on one thread of one server. When a Distributed table probes a *remote* shard
-- for the underlying table's modification_hash (through system.tables), the probe re-enters
-- getModificationHash on a fresh thread of the next server with an empty set. Two Distributed tables that
-- reference each other through a remote shard therefore form a cycle the thread-local set cannot see; without
-- a depth bound on the probe the recursion runs unbounded. The probe must instead fail closed and return NULL.

DROP TABLE IF EXISTS dist_cycle_1;
DROP TABLE IF EXISTS dist_cycle_2;

-- test_cluster_one_shard_remote is a single shard whose only replica is 127.0.0.2, which is never treated as
-- a local address (see isLocalAddress.cpp), so a query to it always takes the remote path on a fresh thread
-- of the same server process - no local shard short-circuits the cycle via the thread-local set. The default
-- (Atomic) database gives the tables a UUID, so getModificationHash does not fail closed on the missing-UUID
-- rule and actually reaches the cross-server probe.
CREATE TABLE dist_cycle_1 (x UInt64) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), 'dist_cycle_2');
CREATE TABLE dist_cycle_2 (x UInt64) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), 'dist_cycle_1');

-- Fail closed: a table caught in a cross-server modification-hash cycle reports NULL. With the depth bound
-- in place this returns promptly; without it the query would recurse until it exhausts connections/threads.
SELECT 'cross-server cycle null (default depth)', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_cycle_1';

-- The bound must not rely on max_distributed_depth: with the depth limit disabled (0) the internal hard cap
-- still terminates the probe, and it fails closed.
SET max_distributed_depth = 0;
SELECT 'cross-server cycle null (unlimited depth)', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_cycle_1';

DROP TABLE dist_cycle_1;
DROP TABLE dist_cycle_2;
