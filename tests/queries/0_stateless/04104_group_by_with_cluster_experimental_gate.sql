-- `GROUP BY ... WITH CLUSTER` is experimental and gated behind
-- `allow_experimental_group_by_with_cluster`. It must be rejected by default on
-- the analyzer path and accepted once the setting is enabled.

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only

-- Disabled by default: rejected during analysis with SUPPORT_IS_DISABLED.
SET allow_experimental_group_by_with_cluster = 0;
SELECT count() FROM VALUES('x UInt64', (1), (2), (3))
GROUP BY x WITH CLUSTER 1; -- { serverError SUPPORT_IS_DISABLED }

-- Enabled: the query runs and chain-merges 1, 2, 3 into a single cluster.
SET allow_experimental_group_by_with_cluster = 1;
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('x UInt64', (1), (2), (3))
    GROUP BY x WITH CLUSTER 1
);
