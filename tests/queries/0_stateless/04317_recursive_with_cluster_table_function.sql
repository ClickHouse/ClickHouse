-- Tags: no-fasttest

-- Regression for #105370: a recursive WITH followed by a cluster table function used to
-- segfault inside `AddDefaultDatabaseVisitor` because `QueryNode::toASTImpl` re-emitted
-- `recursive_with = true` after the analyzer had already cleared the WITH section.

SET enable_analyzer = 1;

WITH RECURSIVE r AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 3
)
SELECT count() FROM clusterAllReplicas('test_cluster_one_shard_two_replicas', view(SELECT 1));
