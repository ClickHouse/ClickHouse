-- Tags: distributed

-- Regression: after `ParserGroupByElement` started wrapping every GROUP BY
-- key in `ASTGroupByElement` (to carry the per-element `WITH CLUSTER`
-- modifier), the non-analyzer
-- `optimize_distributed_group_by_sharding_key` path stopped recognizing
-- bare identifiers under those wrappers, silently disabling the shard-key
-- optimization for ordinary `GROUP BY` queries. The fix unwraps
-- `ASTGroupByElement` in the identifier-extraction lambda. This test
-- proves the optimization still triggers on an ordinary `GROUP BY` under
-- the non-analyzer engine (the EXPLAIN must not contain a `MergingAggregated`
-- step on the initiator).

SET enable_analyzer = 0;
SET optimize_skip_unused_shards = 1;
SET optimize_distributed_group_by_sharding_key = 1;
SET prefer_localhost_replica = 1;

SELECT count(*) AS has_merging_aggregated
FROM (
    EXPLAIN PIPELINE
    SELECT k, sum(v) FROM remote('127.0.0.{1,2}',
        view(SELECT number AS k, number AS v FROM numbers(2)), k)
    GROUP BY k
)
WHERE explain ILIKE '%MergingAggregated%';
