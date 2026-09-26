-- `parallel_replicas_custom_key` may reference an `ALIAS` column, and the custom key filter resolves it
-- to the alias expression. A subquery hidden in such a column must be rejected the same way as one written
-- in the custom key itself, in both modes and over both code paths that build the filter.

DROP TABLE IF EXISTS custom_key_alias;
DROP TABLE IF EXISTS custom_key_alias_set;

CREATE TABLE custom_key_alias_set (id UInt64) ENGINE = Set;
INSERT INTO custom_key_alias_set VALUES (1), (2);

CREATE TABLE custom_key_alias
(
    a UInt64,
    a_in_set ALIAS a IN custom_key_alias_set,
    a_plain ALIAS a * 7
)
ENGINE = MergeTree ORDER BY a;
INSERT INTO custom_key_alias SELECT number FROM numbers(100);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
-- Parallel replicas with a custom key are not implemented with `serialize_query_plan`,
-- and that check fires before the key validation under test.
SET serialize_query_plan = 0;

-- The subquery is still rejected when written in the custom key directly.
SELECT count() FROM custom_key_alias
SETTINGS parallel_replicas_custom_key = 'a IN custom_key_alias_set', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- A subquery hidden in an `ALIAS` column is rejected too.
SELECT count() FROM custom_key_alias
SETTINGS parallel_replicas_custom_key = 'a_in_set', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM custom_key_alias
SETTINGS parallel_replicas_custom_key = 'toUInt64(a_in_set)', parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

-- The same over the `cluster` table function, which builds the custom key filter through `ClusterProxy`.
SELECT count()
FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), custom_key_alias)
SETTINGS parallel_replicas_custom_key = 'a_in_set', parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- An `ALIAS` column without forbidden constructs keeps working in the custom key.
-- The replicas return partial counts, so aggregate them on top.
SELECT sum(c) FROM (SELECT count() AS c FROM custom_key_alias)
SETTINGS parallel_replicas_custom_key = 'a_plain', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE custom_key_alias;
DROP TABLE custom_key_alias_set;
