-- Tags: no-old-analyzer
-- no-old-analyzer: the pruning under test is delivered by `collectFiltersForAnalysis`, which only the
-- analyzer runs, and the old analyzer rewrites a `GLOBAL IN` subquery into an external table that
-- `ReadFromCluster` never sends to the replicas, so the replica fails to resolve it.

-- A cluster read hands paths out to replicas through `getTaskIteratorExtension`, which prunes them
-- with the `_path` / `_file` predicate. The initiator's plan for such a read stops at
-- `WithMergeableState`, so there is no `Filter` step above `ReadFromCluster` to collect the `WHERE`
-- from, and `collectFiltersForAnalysis` used to name only `StorageObjectStorageCluster` among the
-- cluster engines. For `urlCluster` (and for a plain `url` auto-converted to it by
-- `parallel_replicas_for_cluster_engines`) the predicate was therefore always null and no pruning
-- happened: the excluded URL was handed to a replica, which opened it.
--
-- Nothing listens on port 1, so each query below fails with `Connection refused` unless the URL is
-- pruned on the initiator. One try is enough to see that, and it keeps a regression from spending the
-- default ten backing-off retries per replica.
SET http_max_tries = 1;

-- An explicit `urlCluster` with a predicate whose set is ready while the plan is optimized.
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:1/05048_missing.tsv', TSV, 'x UInt64')
WHERE _path = 'no such path';

SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:1/05048_missing.tsv', TSV, 'x UInt64')
WHERE _file = 'no such file';

-- The set of a `GLOBAL IN` may only be created once the pipeline runs, so `DisclosedGlobIterator`
-- has to apply such a predicate when it hands out a URL rather than while it is constructed.
SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:1/05048_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');

SELECT * FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:1/05048_missing.tsv', TSV, 'x UInt64')
WHERE _file GLOBAL IN (SELECT 'no such file');

-- The same for a plain `url` that `parallel_replicas_for_cluster_engines` turns into `StorageURLCluster`.
SELECT * FROM url('http://localhost:1/05048_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path')
SETTINGS parallel_replicas_for_cluster_engines = 1, enable_parallel_replicas = 1, max_parallel_replicas = 2,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT * FROM url('http://localhost:1/05048_missing.tsv', TSV, 'x UInt64')
WHERE _file GLOBAL IN (SELECT 'no such file')
SETTINGS parallel_replicas_for_cluster_engines = 1, enable_parallel_replicas = 1, max_parallel_replicas = 2,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
