SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

SELECT '-- Distributed queries';

WITH t AS MATERIALIZED (SELECT number AS x FROM numbers(7))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (SELECT * FROM t WHERE x > 5) or number IN (SELECT * FROM t WHERE x > 5)
ORDER BY number;

SELECT '---';

WITH t AS MATERIALIZED (SELECT number FROM numbers(7))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (t) or number in (t)
ORDER BY number;

SELECT '-- Distributed broadcast JOIN';

CREATE TABLE fact_local (key UInt64, value String) ENGINE = MergeTree ORDER BY ();
CREATE TABLE fact_dist (key UInt64, value String) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), fact_local);

INSERT INTO fact_local VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Check identifier resolution into column from materialized CTE
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(5))
SELECT key, value FROM fact_dist LEFT JOIN t ON fact_dist.key = t.c
ORDER BY key, value;

SELECT 'NOT INLINED';

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(5))
SELECT key, value FROM fact_dist LEFT JOIN t ON fact_dist.key = t.c
WHERE fact_dist.key IN t
ORDER BY key, value;

SELECT '---';

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(5))
SELECT key, value FROM fact_dist LEFT JOIN t ON fact_dist.key = t.c
WHERE fact_dist.key IN t
ORDER BY key, value;

DROP TABLE IF EXISTS fact_local;
DROP TABLE IF EXISTS fact_dist;

EXPLAIN ESTIMATE
WITH t AS MATERIALIZED (
    SELECT DISTINCT number AS x FROM numbers(isNotNull(assumeNotNull(19)), 7)
)
SELECT DISTINCT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE (number GLOBAL IN (SELECT DISTINCT * FROM t WHERE materialize(5) < x)) OR (number IN (SELECT * FROM t WHERE 5 > x GROUP BY 1 WITH ROLLUP))
ORDER BY ALL ASC;
