-- https://github.com/ClickHouse/ClickHouse/issues/22923
SET enable_analyzer=1;
SET prefer_localhost_replica=0;

create table "t0" (a Int64, b Int64) engine = MergeTree() partition by a order by a;

create table "dist_t0" (a Int64, b Int64) engine = Distributed(test_shard_localhost, currentDatabase(), t0);

insert into t0 values (1, 10), (2, 12);

SELECT * FROM (
WITH
    b AS
    (
         SELECT toInt64(number) AS a
         FROM numbers(10)
    ),
    c AS
    (
         SELECT toInt64(number) AS a
         FROM numbers(10)
    )
SELECT *
FROM dist_t0 AS a
LEFT JOIN b AS b ON a.a = b.a
LEFT JOIN c AS c ON a.a = c.a
)
ORDER BY ALL;
