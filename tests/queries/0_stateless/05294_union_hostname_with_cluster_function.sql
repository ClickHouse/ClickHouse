-- A local GROUP BY hostName() must not be reused for a later cluster branch.
-- hostName() is folded on the local branch and must stay a per-shard column on the cluster branch.

SELECT tag, c, h = hostName()
FROM
(
    SELECT hostName() AS h, count() AS c, 1 AS tag
    FROM numbers(1)
    GROUP BY h
    UNION ALL
    SELECT hostName() AS h, count() AS c, 2 AS tag
    FROM cluster('test_cluster_two_shards', system.one)
    GROUP BY h
)
ORDER BY tag;
