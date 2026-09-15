-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116908
-- `optimize_skip_unused_shards_rewrite_in` prunes the elements of an `IN` set over the sharding
-- column to the ones routed to each shard. That is sound as a filter, but it also changes the name
-- of the expression, and every clause other than the filters carries that name into the header the
-- shard returns: the projection directly, and `GROUP BY` / `ORDER BY` / `LIMIT BY` through the
-- intermediate stages. The initiator binds those columns by name, so the query threw
-- `NOT_FOUND_COLUMN_IN_BLOCK` (`THERE_IS_NO_COLUMN` under the analyzer) instead of returning rows.
-- An aliased `IN` is skipped as well: the alias is shared with (or expanded into) the other clauses,
-- which otherwise leaves two different expressions behind the same alias.
--
-- Both shards of `test_cluster_two_shards` read the same `system.one`, which does not respect the
-- sharding key, so the pruning legitimately changes how many rows come back.

SET prefer_localhost_replica = 0;
SET optimize_skip_unused_shards = 1;
SET optimize_skip_unused_shards_rewrite_in = 1;

DROP TABLE IF EXISTS dist_05071;
CREATE TABLE dist_05071 AS system.one ENGINE = Distributed(test_cluster_two_shards, system, one, intHash64(dummy));

-- intHash64(0) % 2 = 0 and intHash64(2) % 2 = 1, so both shards are queried and each one prunes the
-- set in its `WHERE` down to a single element - unless the same expression also occurs outside the
-- filters. The old analyzer binds the remote columns by name and `WHERE` can be the only place that
-- computes one of them, so there the rewrite is skipped for such shapes and both shards keep their
-- row; the analyzer resolves the columns by node and still prunes. Hence the two blocks below differ
-- in the number of rows, not in whether the queries work.

SET enable_analyzer = 0;
SELECT 'old analyzer';

SELECT dummy, dummy IN (0, 2) FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy;
SELECT dummy IN (0, 2) FROM dist_05071 ORDER BY dummy;
SELECT dummy, dummy IN (0, 2), dummy IN (1, 3) FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy;
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2) GROUP BY dummy IN (0, 2);
SELECT dummy FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy IN (0, 2), dummy;
SELECT dummy FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy LIMIT 1 BY dummy IN (0, 2);
SELECT dummy, (dummy IN (0, 2)) AS f FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy, f;
SELECT (dummy IN (0, 2)) AS f, dummy FROM dist_05071 WHERE (dummy IN (0, 2)) AS f ORDER BY dummy;
SELECT dummy, (dummy IN (0, 2)) AS f FROM dist_05071 WHERE f ORDER BY dummy;

-- A filter-only shape keeps being rewritten: shard 1 gets `IN (0)` and shard 2 gets `IN (2)`, so
-- only one of them has a matching row, while without the rewrite both do.
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2);
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2) SETTINGS optimize_skip_unused_shards_rewrite_in = 0;

SET enable_analyzer = 1;
SELECT 'analyzer';

SELECT dummy, dummy IN (0, 2) FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy;
SELECT dummy IN (0, 2) FROM dist_05071 ORDER BY dummy;
SELECT dummy, dummy IN (0, 2), dummy IN (1, 3) FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy;
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2) GROUP BY dummy IN (0, 2);
SELECT dummy FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy IN (0, 2), dummy;
SELECT dummy FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy LIMIT 1 BY dummy IN (0, 2);
SELECT dummy, (dummy IN (0, 2)) AS f FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy, f;
SELECT (dummy IN (0, 2)) AS f, dummy FROM dist_05071 WHERE (dummy IN (0, 2)) AS f ORDER BY dummy;
SELECT dummy, (dummy IN (0, 2)) AS f FROM dist_05071 WHERE f ORDER BY dummy;

SELECT count() FROM dist_05071 WHERE dummy IN (0, 2);
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2) SETTINGS optimize_skip_unused_shards_rewrite_in = 0;

DROP TABLE dist_05071;
