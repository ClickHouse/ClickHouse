SET enable_analyzer = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_dist;

CREATE TABLE t
(
    `id` int,
    `a` int,
    `b` int
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t VALUES (1, 2, 3);

CREATE TABLE t_dist AS t
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t, id);

SELECT a
FROM
(
    SELECT
        a,
        b,
        count(*) AS v
    FROM t_dist
    GROUP BY ALL
) AS Z;

DROP TABLE t_dist;
DROP TABLE t;
