-- Regression: a removable constant key before the WITH CLUSTER key used to
-- shift the post-Planner `aggregation_keys` indices, silently disabling
-- clustering (or pointing it at the wrong column).

-- Constant before cluster key: clustering must still apply to `x`.
SELECT count() AS num_clusters, max(c) AS max_cluster_size
FROM (
    SELECT x, count() AS c
    FROM (SELECT toUInt64(number) AS x FROM numbers(10))
    GROUP BY 'lit', x WITH CLUSTER 1
);

-- Two constants before cluster key: same expectation.
SELECT count() AS num_clusters, max(c) AS max_cluster_size
FROM (
    SELECT x, count() AS c
    FROM (SELECT toUInt64(number) AS x FROM numbers(10))
    GROUP BY 'a', 'b', x WITH CLUSTER 1
);

-- Constant after cluster key — no shift, but should still work.
SELECT count() AS num_clusters, max(c) AS max_cluster_size
FROM (
    SELECT x, count() AS c
    FROM (SELECT toUInt64(number) AS x FROM numbers(10))
    GROUP BY x WITH CLUSTER 1, 'lit'
);

-- WITH CLUSTER on a constant key is meaningless and must be rejected.
SELECT count() FROM (SELECT toUInt64(number) AS x FROM numbers(10))
GROUP BY 'lit' WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
