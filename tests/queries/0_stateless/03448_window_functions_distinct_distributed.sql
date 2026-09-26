-- Tags: distributed

SET distributed_aggregation_memory_efficient = 1;

SELECT any(total) AS total_distinct_avg
FROM (
    SELECT number,
        avgDistinct(number) OVER () AS total
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT max(running_avg) AS final_running_avg
FROM (
    SELECT number,
        avgDistinct(number) OVER (ORDER BY number) AS running_avg
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    arraySort(groupUniqArray((partition_mod, max_val))) AS max_strings_per_partition
FROM (
    SELECT
        number % 10 AS partition_mod,
        maxDistinct(toString(number % 5)) OVER (PARTITION BY number % 7) AS max_val
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    arraySort(groupUniqArray((partition_mod, argMin_val))) AS argmin_per_partition
FROM (
    SELECT
        number                             AS ts,
        ts % 10                            AS partition_mod,
        toString(ts % 5)                   AS s,
        argMinDistinct(ts, s) OVER (PARTITION BY ts % 3 ORDER BY ts) AS argMin_val
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000)) ORDER BY ts
);

SET distributed_aggregation_memory_efficient = 0;

SELECT any(total) AS total_distinct_avg
FROM (
    SELECT number,
        avgDistinct(number) OVER () AS total
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT max(running_avg) AS final_running_avg
FROM (
    SELECT number,
        avgDistinct(number) OVER (ORDER BY number) AS running_avg
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    arraySort(groupUniqArray((partition_mod, max_val))) AS max_strings_per_partition
FROM (
    SELECT
        number % 10 AS partition_mod,
        maxDistinct(toString(number % 5)) OVER (PARTITION BY number % 7) AS max_val
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000))
);

SELECT
    arraySort(groupUniqArray((partition_mod, argMin_val))) AS argmin_per_partition
FROM (
    SELECT
        number                             AS ts,
        ts % 10                            AS partition_mod,
        toString(ts % 5)                   AS s,
        argMinDistinct(ts, s) OVER (PARTITION BY ts % 3 ORDER BY ts) AS argMin_val
    FROM remote('127.0.0.{1,2,3}', numbers_mt(100_000)) ORDER BY ts
);
