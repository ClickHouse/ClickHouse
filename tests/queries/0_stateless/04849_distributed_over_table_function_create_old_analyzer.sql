-- Tags: shard
-- Creating a `Distributed` (or `Remote`) table over a table function with a scalar-subquery argument
-- must work in a session with the analyzer disabled: the create-time execution of the target (the
-- access-check probe for a cluster with a local shard) runs with the analyzer forced on, because the
-- legacy path of `evaluateConstantExpressionAsColumn` cannot execute a scalar subquery in a table
-- function argument ("result column not found"). Only the CREATE is exercised under the old analyzer;
-- reads of such a target are analyzer-only by design (see
-- 04817_distributed_over_remote_table_function_binding).
SET enable_analyzer = 0;
CREATE TABLE subq_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO subq_src VALUES (1), (2), (3);
CREATE TABLE dist_subq ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM subq_src))));
CREATE TABLE remote_subq (number UInt64) ENGINE = Remote('127.0.0.1', numbers(assumeNotNull((SELECT count() FROM subq_src))));
SELECT count() FROM dist_subq SETTINGS enable_analyzer = 1;
SELECT count() FROM remote_subq SETTINGS enable_analyzer = 1;
DROP TABLE remote_subq;
DROP TABLE dist_subq;
DROP TABLE subq_src;
