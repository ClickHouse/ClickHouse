SELECT
    k1,
    k2,
    SUM(number) AS sum_value,
    count() AS count_value
FROM numbers(6)
GROUP BY
    GROUPING SETS
    (
        (number % 2 AS k1),
        (number % 3 AS k2)
    )
ORDER BY
    sum_value ASC,
    count_value ASC;

SELECT
    k1,
    k2,
    SUM(number) AS sum_value,
    count() AS count_value
FROM remote('127.0.0.{2,3}', numbers(6))
GROUP BY
    GROUPING SETS
    (
        (number % 2 AS k1),
        (number % 3 AS k2)
    )
ORDER BY
    sum_value ASC,
    count_value ASC;

SELECT
    k2,
    SUM(number) AS sum_value,
    count() AS count_value
FROM remote('127.0.0.{2,3}', numbers(6))
GROUP BY
    GROUPING SETS
    (
        (number % 3 AS k2)
    )
ORDER BY
    sum_value ASC,
    count_value ASC;

set prefer_localhost_replica = 1;

-- { echo On }

SELECT count(), arrayMap(x -> '.', range(number % 10)) AS k FROM remote('127.0.0.{1,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), arrayMap(x -> '.', range(number % 10)) AS k FROM remote('127.0.0.{1,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (k, k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;

SELECT count(), toString(number) AS k FROM remote('127.0.0.{1,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), toString(number) AS k FROM remote('127.0.0.{1,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (k, k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), toString(number) AS k FROM remote('127.0.0.{1,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (number + 1, k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), toString(number) AS k FROM remote('127.0.0.{1,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (number + 1, k), (k, number + 2)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;

SELECT count(), arrayMap(x -> '.', range(number % 10)) AS k FROM remote('127.0.0.{3,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), arrayMap(x -> '.', range(number % 10)) AS k FROM remote('127.0.0.{3,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (k, k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;

SELECT count(), toString(number) AS k FROM remote('127.0.0.{3,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), toString(number) AS k FROM remote('127.0.0.{3,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (k, k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), toString(number) AS k FROM remote('127.0.0.{3,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (number + 1, k)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
SELECT count(), toString(number) AS k FROM remote('127.0.0.{3,2}', numbers(10)) where number > ( queryID() = initialQueryID()) GROUP BY GROUPING SETS ((k), (number + 1, k), (k, number + 2)) ORDER BY k settings group_by_two_level_threshold=9, max_bytes_before_external_group_by=10000000000;
