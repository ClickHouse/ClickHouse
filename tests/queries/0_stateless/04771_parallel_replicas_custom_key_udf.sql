-- Tags: no-parallel
-- - no-parallel -- SQL UDFs are global server objects; the flaky check runs the same test concurrently and the CREATE FUNCTION statements would collide.

-- `parallel_replicas_custom_key` in `custom_key_range` mode goes through `KeyDescription::getKeyFromAST`,
-- which forbids subqueries and column matchers in key expressions. Unlike the DDL carriers
-- (`CREATE TABLE` / `ALTER TABLE`), the custom key arrives as a setting string that is parsed on the spot,
-- so SQL UDFs must be inlined at that parse site as well - a UDF body must not hide a forbidden
-- construct from the validation.

DROP FUNCTION IF EXISTS f_04771_in_set;
DROP FUNCTION IF EXISTS f_04771_matcher;
DROP FUNCTION IF EXISTS f_04771_plain;
DROP TABLE IF EXISTS custom_key_udf_src;
DROP TABLE IF EXISTS custom_key_udf;

CREATE TABLE custom_key_udf_src (id UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE custom_key_udf (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO custom_key_udf SELECT number, number % 3 FROM numbers(100);

CREATE FUNCTION f_04771_in_set AS x -> x IN (SELECT id FROM custom_key_udf_src);
CREATE FUNCTION f_04771_matcher AS x -> (COLUMNS('^a$'), x);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_mode = 'custom_key_range';
-- Parallel replicas with a custom key are not implemented with `serialize_query_plan`,
-- and that check fires before the key validation under test.
SET serialize_query_plan = 0;

-- A subquery hidden in a UDF is rejected in the custom key.
SELECT count() FROM custom_key_udf
SETTINGS parallel_replicas_custom_key = 'f_04771_in_set(a)'; -- { serverError BAD_ARGUMENTS }

-- The same for a column matcher hidden in a UDF, even one that matches exactly one column.
SELECT count() FROM custom_key_udf
SETTINGS parallel_replicas_custom_key = 'f_04771_matcher(b)'; -- { serverError BAD_ARGUMENTS }

-- The same over the `cluster` table function, which builds the custom key filter
-- through a different code path (`ClusterProxy`).
SELECT count()
FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), custom_key_udf)
SETTINGS parallel_replicas_custom_key = 'f_04771_in_set(a)'; -- { serverError BAD_ARGUMENTS }

-- A UDF without forbidden constructs keeps working in the custom key, in both modes.
-- The replicas return partial counts, so aggregate them on top.
CREATE FUNCTION f_04771_plain AS x -> x % 10;

SELECT sum(c) FROM (SELECT count() AS c FROM custom_key_udf)
SETTINGS parallel_replicas_custom_key = 'f_04771_plain(a)';

SELECT sum(c) FROM (SELECT count() AS c FROM custom_key_udf)
SETTINGS parallel_replicas_custom_key = 'f_04771_plain(a)', parallel_replicas_mode = 'custom_key_sampling';

DROP TABLE custom_key_udf;
DROP TABLE custom_key_udf_src;

DROP FUNCTION f_04771_plain;
DROP FUNCTION f_04771_matcher;
DROP FUNCTION f_04771_in_set;
