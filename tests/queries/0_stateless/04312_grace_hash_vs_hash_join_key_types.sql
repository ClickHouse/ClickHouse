-- Tags: no-random-settings
--
-- grace_hash routes rows to buckets via IColumn::computeHashInto (JoinUtils::scatterBlockByHash).
-- If a key column's computeHashInto mis-routes equal keys, grace_hash silently drops join
-- matches while the default hash join does not. This test asserts grace_hash == hash for the
-- key families whose computeHashInto this PR changed: UInt, String, Nullable, LowCardinality,
-- Array, Tuple. Each family compares an order-independent checksum (sum of per-row cityHash64)
-- of an ALL INNER JOIN run under grace_hash (forced into multiple hash-routed buckets) vs hash.

SELECT 'uint64' AS t,
    (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT number % 5000 AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT number AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 2000000, grace_hash_join_initial_buckets = 4)
    = (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT number % 5000 AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT number AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'hash') AS ok;

SELECT 'string' AS t,
    (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT toString(number % 5000) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT toString(number) AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 2000000, grace_hash_join_initial_buckets = 4)
    = (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT toString(number % 5000) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT toString(number) AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'hash') AS ok;

SELECT 'nullable' AS t,
    (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT if(number % 9 = 0, NULL, number % 5000) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT number AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 2000000, grace_hash_join_initial_buckets = 4)
    = (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT if(number % 9 = 0, NULL, number % 5000) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT number AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'hash') AS ok;

SELECT 'lowcardinality' AS t,
    (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT toLowCardinality(toString(number % 5000)) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT toLowCardinality(toString(number)) AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 2000000, grace_hash_join_initial_buckets = 4)
    = (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT toLowCardinality(toString(number % 5000)) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT toLowCardinality(toString(number)) AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'hash') AS ok;

SELECT 'array' AS t,
    (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT [number % 5000, number % 7] AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT [number, number % 7] AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 2000000, grace_hash_join_initial_buckets = 4)
    = (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT [number % 5000, number % 7] AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT [number, number % 7] AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'hash') AS ok;

SELECT 'tuple' AS t,
    (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT (number % 5000, toString(number % 3)) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT (number, toString(number % 3)) AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'grace_hash', max_bytes_in_join = 2000000, grace_hash_join_initial_buckets = 4)
    = (SELECT sum(cityHash64(a.k, b.x)) FROM (SELECT (number % 5000, toString(number % 3)) AS k FROM numbers(20000)) AS a ALL INNER JOIN (SELECT (number, toString(number % 3)) AS k, number * 7 AS x FROM numbers(5000)) AS b ON a.k = b.k
        SETTINGS join_algorithm = 'hash') AS ok;
