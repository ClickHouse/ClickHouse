-- Companion of 04494_atomic_populate_materialized_view_parallel_replicas, for parallel-replica settings
-- specified in the view's own SELECT instead of the session. Query-local SETTINGS are reapplied on top of
-- the query context by both the analyzer and the old interpreter, so they must not be able to send the
-- atomic population read to remote replicas, which do not carry the pinned local snapshot.

SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_only_with_analyzer = 0; -- necessary for CI run with disabled analyzer
SET parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(1000);

CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS
    SELECT id FROM src
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
             cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
             parallel_distributed_insert_select = 2, enable_shared_storage_snapshot_in_query = 1;

SELECT 'query_local_settings', count(), uniqExact(id), sum(id) FROM mv;

DROP TABLE mv;

-- The same via the legacy alias of `enable_parallel_replicas`, and inside a nested subquery.
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS
    SELECT id FROM (
        SELECT id FROM src
        SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3,
                 cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'
    );

SELECT 'query_local_settings_nested', count(), uniqExact(id), sum(id) FROM mv;

DROP TABLE mv;

-- The `name = DEFAULT` carrier lives in a separate list of the `SETTINGS` clause, so it has to be scrubbed
-- too. It is not a harmless no-op: `DEFAULT` restores the built-in default, and for two of these settings
-- that default is the dangerous value (`parallel_distributed_insert_select = 2`,
-- `enable_shared_storage_snapshot_in_query = true`), which would undo what the population context forces.
-- Here the clause holds nothing but scrubbed settings, so it is detached entirely - had it been left empty,
-- the population query would format to a bare `SETTINGS` keyword that fails to re-parse.
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS
    SELECT id FROM src
    SETTINGS parallel_distributed_insert_select = DEFAULT, enable_shared_storage_snapshot_in_query = DEFAULT;

SELECT 'query_local_settings_default', count(), uniqExact(id), sum(id) FROM mv;

DROP TABLE mv;
DROP TABLE src;
