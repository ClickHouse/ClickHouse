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
-- The branch these two arms cover is the deferred filter of the local
-- `ReadFromURL` plan, so `parallel_replicas_for_cluster_engines` is pinned off
-- to ask for that plan explicitly instead of depending on how the run
-- randomizes replicas. `StorageURLCluster` builds the same iterator from
-- `getTaskIteratorExtension`; the cluster plan is covered by the arm above
-- (which deliberately keeps it reachable) and by
-- `05048_url_cluster_path_filter_prune`.
SELECT * FROM url('http://localhost:1/04907_missing.tsv', TSV, 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path')
SETTINGS parallel_replicas_for_cluster_engines = 0;

SELECT * FROM url('http://localhost:1/04907_missing.tsv', TSV, 'x UInt64')
WHERE _file GLOBAL IN (SELECT 'no such file')
SETTINGS parallel_replicas_for_cluster_engines = 0;
