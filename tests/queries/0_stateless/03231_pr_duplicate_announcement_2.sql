DROP TABLE IF EXISTS ANIMAL SYNC;

CREATE TABLE ANIMAL ( ANIMAL Nullable(String) ) ENGINE = ReplicatedMergeTree('/clickhouse/test/{database}/animal', 'r1') ORDER BY tuple();

INSERT INTO ANIMAL (ANIMAL) VALUES ('CAT'), ('FISH'), ('DOG'), ('HORSE'), ('BIRD');

SET joined_subquery_requires_alias = 0;
SET allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT *
FROM
(
    SELECT
        x.b AS x,
        countDistinct(x.c) AS ANIMAL
    FROM
    (
        SELECT
            a.ANIMAL AS a,
            'CAT' AS b,
            c.ANIMAL AS c,
            d.ANIMAL AS d
        FROM ANIMAL AS a
        INNER JOIN ANIMAL AS b ON a.ANIMAL = b.ANIMAL
        LEFT JOIN ANIMAL AS c ON b.ANIMAL = c.ANIMAL
        RIGHT JOIN
        (
            SELECT *
            FROM ANIMAL
            UNION ALL
            SELECT *
            FROM ANIMAL
            UNION ALL
            SELECT *
            FROM ANIMAL
        ) AS d ON a.ANIMAL = d.ANIMAL
        WHERE (d.ANIMAL != 'CAT') AND (c.ANIMAL != 'DOG') AND (b.ANIMAL != 'FISH')
    ) AS x
    WHERE x.b >= 'CAT'
    GROUP BY x.b
    HAVING ANIMAL >= 0
) AS ANIMAL
WHERE ANIMAL.ANIMAL >= 0;

DROP TABLE ANIMAL SYNC;
