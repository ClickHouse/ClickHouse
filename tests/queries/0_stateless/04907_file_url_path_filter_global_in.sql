-- The eager `_path` / `_file` pruning for `file()` and `url()` runs while the
-- pipeline is being built. A `GLOBAL IN` set can only be created when the
-- pipeline runs, so the iterator has to defer the pruning and apply it before
-- trying to open the excluded input.
SELECT * FROM file('04907_file_path_filter_global_in_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');

-- A comma in a plain URL is literal, not a separator for multiple URLs.
SELECT * FROM url('http://localhost:8123/?query=SELECT%201%20FORMAT%20TSV&custom_comment=a,b', TSV, 'x UInt64')
WHERE _path = '/';

-- A non-glob `url()` defers the pruning too: the excluded URL must never be
-- opened. Nothing listens on port 1, so the query fails if a connection is
-- attempted.
--
-- `parallel_replicas_for_cluster_engines` is pinned off for these two arms:
-- with parallel replicas enabled `url` is served by `StorageURLCluster`, whose
-- task iterator hands the URL out to a replica without applying this pruning,
-- and the replica then opens the URL and fails with `Connection refused`. The
-- deferral being checked here belongs to the local plan, so ask for it
-- explicitly instead of depending on how the run randomizes replicas. The
-- arm above deliberately keeps the cluster plan reachable, because that is
-- where `StorageURLCluster` builds the same iterator.
SELECT * FROM url('http://localhost:1/04907_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path')
SETTINGS parallel_replicas_for_cluster_engines = 0;

SELECT * FROM url('http://localhost:1/04907_missing.tsv', TSV, 'x UInt64')
WHERE _file GLOBAL IN (SELECT 'no such file')
SETTINGS parallel_replicas_for_cluster_engines = 0;
