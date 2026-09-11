SET optimize_on_insert = 1;
SET max_threads = 1;
SET max_block_size = 100000;
SET max_insert_block_size = 100000;
SET min_insert_block_size_rows = 100000;

DROP TABLE IF EXISTS collapsing_stable_sort;
CREATE TABLE collapsing_stable_sort
(
    id Int64,
    type Int64,
    status Int8,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (type, id);

-- 40000 keys: large equal ranges exercise radix sorting.
INSERT INTO collapsing_stable_sort
SELECT 101000000000000001 + number, number % 64 + 1, 10, 1
FROM numbers(40000);

-- Keep cancellation before replacement, but do not order by the table sorting key.
INSERT INTO collapsing_stable_sort
SELECT 101000000000000001 + number AS id, number % 64 + 1,
       if(sign = -1, 10, 20), sign
FROM numbers(40000)
ARRAY JOIN [-1, 1] AS sign
ORDER BY id, sign;

-- Row count, distinct keys, key domain and row state together rule out
-- missing keys, duplicate keys, stale rows and unexpected rows.
SELECT count(), uniqExact(tuple(type, id)),
       countIf(status != 20 OR sign != 1),
       countIf(id < 101000000000000001 OR id >= 101000000000000001 + 40000
               OR type != (id - 101000000000000001) % 64 + 1)
FROM collapsing_stable_sort FINAL;

TRUNCATE TABLE collapsing_stable_sort;

-- 64 keys: small equal ranges exercise comparison sorting.
INSERT INTO collapsing_stable_sort
SELECT 101000000000000001 + number, number % 64 + 1, 10, 1
FROM numbers(64);

-- Keep cancellation before replacement, but do not order by the table sorting key.
INSERT INTO collapsing_stable_sort
SELECT 101000000000000001 + number AS id, number % 64 + 1,
       if(sign = -1, 10, 20), sign
FROM numbers(64)
ARRAY JOIN [-1, 1] AS sign
ORDER BY id, sign;

-- Row count, distinct keys, key domain and row state together rule out
-- missing keys, duplicate keys, stale rows and unexpected rows.
SELECT count(), uniqExact(tuple(type, id)),
       countIf(status != 20 OR sign != 1),
       countIf(id < 101000000000000001 OR id >= 101000000000000001 + 64
               OR type != (id - 101000000000000001) % 64 + 1)
FROM collapsing_stable_sort FINAL;

TRUNCATE TABLE collapsing_stable_sort;

DROP TABLE collapsing_stable_sort;
