-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/108291
-- SELECT * with asterisk_include_alias_columns=1 over a cluster/Distributed table plus an
-- ORDER BY (so the query runs to a mergeable stage) failed with
-- NUMBER_OF_COLUMNS_DOESNT_MATCH when the table had an ALIAS column whose defining
-- expression is a plain reference to another column (e.g. `c ALIAS a`). Such an alias
-- collapsed onto its source column in the shard read plan, so the initiator computed a
-- header with fewer columns than the outer planner expected. A query that reaches the
-- server without throwing is the regression guard here.
-- DISTINCT keeps the output independent of the cluster topology: cluster() reads every
-- shard while clusterAllReplicas() may treat same-address shards as replicas, so the raw
-- row multiplicity differs between environments; the column set and values do not.

DROP TABLE IF EXISTS t_alias_ref;

CREATE TABLE t_alias_ref
(
    a UInt64,
    b String,
    c UInt64 ALIAS a,          -- plain column reference (the trigger)
    d String ALIAS b,          -- plain column reference, non-numeric
    e UInt64 ALIAS a * 10,     -- function alias (must keep working)
    f UInt64 ALIAS c           -- alias of an alias
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO t_alias_ref VALUES (1, 'x') (2, 'y');

-- The bug and its fix live in the analyzer shard-collapse path. StorageDistributed::read
-- has a separate old-analyzer branch, so without pinning enable_analyzer the test can run
-- through the legacy planner and never exercise the guarded alias-collapse path.
SET enable_analyzer = 1;
SET asterisk_include_alias_columns = 1;
SET prefer_localhost_replica = 0;

SELECT 'clusterAllReplicas, SELECT *, ORDER BY column';
SELECT DISTINCT * FROM clusterAllReplicas('test_cluster_two_shards_localhost', currentDatabase(), t_alias_ref)
ORDER BY a, b;

SELECT 'clusterAllReplicas, SELECT *, ORDER BY tuple()';
SELECT * FROM clusterAllReplicas('test_cluster_two_shards_localhost', currentDatabase(), t_alias_ref)
ORDER BY tuple()
LIMIT 0;

SELECT 'cluster(), SELECT *, ORDER BY column';
SELECT DISTINCT * FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_alias_ref)
ORDER BY a, b;

SELECT 'cluster(), explicit alias columns, ORDER BY';
SELECT DISTINCT a, b, c, d, e, f
FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_alias_ref)
ORDER BY a, b;

DROP TABLE t_alias_ref;
