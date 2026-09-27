SET optimize_on_insert = 1;
SET max_threads = 1;
SET max_block_size = 100000;
SET max_insert_block_size = 100000;
SET min_insert_block_size_rows = 100000;

DROP TABLE IF EXISTS collapsing_stable_decimal;
CREATE TABLE collapsing_stable_decimal
(
    id Decimal64(4),
    type Decimal64(4),
    status Int8,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (type, id);

-- 40000 keys: large equal ranges exercise radix sorting.
INSERT INTO collapsing_stable_decimal
SELECT toDecimal64(1000001 + number, 4) / 10000, toDecimal64(number % 64 + 1, 4) / 10000, 10, 1
FROM numbers(40000);

-- Keep cancellation before replacement, but do not order by the table sorting key.
INSERT INTO collapsing_stable_decimal
SELECT toDecimal64(1000001 + number, 4) / 10000 AS id, toDecimal64(number % 64 + 1, 4) / 10000, if(sign = -1, 10, 20), sign
FROM numbers(40000)
ARRAY JOIN [-1, 1] AS sign
ORDER BY id, sign;

-- Check complete expected keys and states, including missing or unexpected keys.
SELECT count(), uniqExact(tuple(type, id)), countIf(status != 20 OR sign != 1),
       countIf((type, id) NOT IN
           (SELECT toDecimal64(number % 64 + 1, 4) / 10000, toDecimal64(1000001 + number, 4) / 10000 FROM numbers(40000)))
FROM collapsing_stable_decimal FINAL;

TRUNCATE TABLE collapsing_stable_decimal;

-- 100 keys: unsorted input below the radix threshold exercises comparison sorting.
INSERT INTO collapsing_stable_decimal
SELECT toDecimal64(1000001 + number, 4) / 10000, toDecimal64(number % 64 + 1, 4) / 10000, 10, 1
FROM numbers(100);

-- Keep cancellation before replacement, but do not order by the table sorting key.
INSERT INTO collapsing_stable_decimal
SELECT toDecimal64(1000001 + number, 4) / 10000 AS id, toDecimal64(number % 64 + 1, 4) / 10000, if(sign = -1, 10, 20), sign
FROM numbers(100)
ARRAY JOIN [-1, 1] AS sign
ORDER BY id, sign;

-- Check complete expected keys and states, including missing or unexpected keys.
SELECT count(), uniqExact(tuple(type, id)), countIf(status != 20 OR sign != 1),
       countIf((type, id) NOT IN
           (SELECT toDecimal64(number % 64 + 1, 4) / 10000, toDecimal64(1000001 + number, 4) / 10000 FROM numbers(100)))
FROM collapsing_stable_decimal FINAL;

TRUNCATE TABLE collapsing_stable_decimal;

DROP TABLE collapsing_stable_decimal;

DROP TABLE IF EXISTS collapsing_stable_datetime64;
CREATE TABLE collapsing_stable_datetime64
(
    id DateTime64(3, 'UTC'),
    type DateTime64(3, 'UTC'),
    status Int8,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (type, id);

-- 40000 keys: large equal ranges exercise radix sorting.
INSERT INTO collapsing_stable_datetime64
SELECT fromUnixTimestamp64Milli(toInt64(1000001 + number)), fromUnixTimestamp64Milli(toInt64(number % 64 + 1)), 10, 1
FROM numbers(40000);

-- Keep cancellation before replacement, but do not order by the table sorting key.
INSERT INTO collapsing_stable_datetime64
SELECT fromUnixTimestamp64Milli(toInt64(1000001 + number)) AS id, fromUnixTimestamp64Milli(toInt64(number % 64 + 1)), if(sign = -1, 10, 20), sign
FROM numbers(40000)
ARRAY JOIN [-1, 1] AS sign
ORDER BY id, sign;

-- Check complete expected keys and states, including missing or unexpected keys.
SELECT count(), uniqExact(tuple(type, id)), countIf(status != 20 OR sign != 1),
       countIf((type, id) NOT IN
           (SELECT fromUnixTimestamp64Milli(toInt64(number % 64 + 1)), fromUnixTimestamp64Milli(toInt64(1000001 + number)) FROM numbers(40000)))
FROM collapsing_stable_datetime64 FINAL;

TRUNCATE TABLE collapsing_stable_datetime64;

-- 100 keys: unsorted input below the radix threshold exercises comparison sorting.
INSERT INTO collapsing_stable_datetime64
SELECT fromUnixTimestamp64Milli(toInt64(1000001 + number)), fromUnixTimestamp64Milli(toInt64(number % 64 + 1)), 10, 1
FROM numbers(100);

-- Keep cancellation before replacement, but do not order by the table sorting key.
INSERT INTO collapsing_stable_datetime64
SELECT fromUnixTimestamp64Milli(toInt64(1000001 + number)) AS id, fromUnixTimestamp64Milli(toInt64(number % 64 + 1)), if(sign = -1, 10, 20), sign
FROM numbers(100)
ARRAY JOIN [-1, 1] AS sign
ORDER BY id, sign;

-- Check complete expected keys and states, including missing or unexpected keys.
SELECT count(), uniqExact(tuple(type, id)), countIf(status != 20 OR sign != 1),
       countIf((type, id) NOT IN
           (SELECT fromUnixTimestamp64Milli(toInt64(number % 64 + 1)), fromUnixTimestamp64Milli(toInt64(1000001 + number)) FROM numbers(100)))
FROM collapsing_stable_datetime64 FINAL;

TRUNCATE TABLE collapsing_stable_datetime64;

DROP TABLE collapsing_stable_datetime64;
