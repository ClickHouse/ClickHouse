-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/108299
-- optimize_functions_to_subcolumns rewrote name != '' -> name.size on the initiator against the
-- Distributed table and serialized that form to the shards. The local table column is
-- LowCardinality(String) while the Distributed table declares it as String, so the rewritten
-- subcolumn __table1.name.size failed to resolve on the shard (UNKNOWN_IDENTIFIER, code 47).
-- StorageDistributed now returns supportsOptimizationToSubcolumns() = false, so the rewrite no
-- longer fires on the initiator; the shard re-analyzes the query against the real table.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_users_108299;
DROP TABLE IF EXISTS t_users_dist_108299;

CREATE TABLE t_users_108299 (uid Int16, name LowCardinality(String), age Int16) ENGINE = Memory;
CREATE TABLE t_users_dist_108299 (uid Int16, name String, age Int16)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_users_108299);

INSERT INTO t_users_108299 VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

-- optimize_empty_string_comparisons must be pinned: the buggy rewrite only fires via
-- name != '' -> notEmpty(name) -> name.size, which requires this setting enabled. CI
-- randomizes it, so pin it here to keep this a deterministic regression guard.
SELECT age FROM t_users_dist_108299 WHERE name != '' ORDER BY age SETTINGS optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1;
SELECT age FROM t_users_dist_108299 WHERE name != '' ORDER BY age SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 1;

DROP TABLE t_users_dist_108299;
DROP TABLE t_users_108299;
