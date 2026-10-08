-- { echoOn }

DROP TABLE IF EXISTS distinct_lc_basic;
CREATE TABLE distinct_lc_basic
(
    id UInt32,
    s String,
    lc LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO distinct_lc_basic
SELECT
    number,
    toString(intDiv(number, 10)),
    toLowCardinality(toString(intDiv(number, 10)))
FROM numbers(100000);

SELECT
    (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_basic))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_basic));

SELECT
    (SELECT arraySort(groupArray(s)) FROM (SELECT DISTINCT s FROM distinct_lc_basic))
    =
    (SELECT arraySort(groupArray(CAST(lc AS String))) FROM (SELECT DISTINCT lc FROM distinct_lc_basic));

DROP TABLE IF EXISTS distinct_lc_low_cardinality;
CREATE TABLE distinct_lc_low_cardinality
(
    id UInt32,
    s String,
    lc LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO distinct_lc_low_cardinality
SELECT
    number,
    toString(number % 3),
    toLowCardinality(toString(number % 3))
FROM numbers(100000);

SELECT
    (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_low_cardinality))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_low_cardinality));

SELECT
    (SELECT arraySort(groupArray(s)) FROM (SELECT DISTINCT s FROM distinct_lc_low_cardinality))
    =
    (SELECT arraySort(groupArray(CAST(lc AS String))) FROM (SELECT DISTINCT lc FROM distinct_lc_low_cardinality));

DROP TABLE IF EXISTS distinct_lc_nullable;
CREATE TABLE distinct_lc_nullable
(
    id UInt32,
    s Nullable(String),
    lc LowCardinality(Nullable(String))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO distinct_lc_nullable
SELECT
    number,
    if(number % 10 = 0, NULL, toString(intDiv(number, 7))),
    toLowCardinality(if(number % 10 = 0, NULL, toString(intDiv(number, 7))))
FROM numbers(100000);

SELECT
    (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_nullable))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_nullable));

SELECT
    (SELECT arraySort(groupArray(s)) FROM (SELECT DISTINCT s FROM distinct_lc_nullable))
    =
    (SELECT arraySort(groupArray(lc)) FROM (SELECT DISTINCT lc FROM distinct_lc_nullable));

SELECT
    (SELECT count() FROM (SELECT DISTINCT lc, 1 FROM distinct_lc_basic))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_basic));

SELECT
    (SELECT count() FROM (SELECT DISTINCT 1, lc FROM distinct_lc_basic))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_basic));

SELECT
    (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_basic SETTINGS max_block_size = 128))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_basic SETTINGS max_block_size = 8192));

SELECT
    (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_basic SETTINGS max_block_size = 128))
  - (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_basic SETTINGS max_block_size = 8192));

SELECT
    (SELECT count() FROM (SELECT DISTINCT toString(intDiv(number, 7)) AS x FROM numbers(100000)))
  - (SELECT count() FROM (SELECT DISTINCT toLowCardinality(toString(intDiv(number, 7))) AS x FROM numbers(100000)));

SELECT
    (SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT toString(intDiv(number, 7)) AS x FROM numbers(100000)))
    =
    (SELECT arraySort(groupArray(CAST(x AS String))) FROM (SELECT DISTINCT toLowCardinality(toString(intDiv(number, 7))) AS x FROM numbers(100000)));

DROP TABLE IF EXISTS distinct_lc_mixed;
CREATE TABLE distinct_lc_mixed
(
    id UInt32,
    k1 LowCardinality(String),
    k2 UInt32
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO distinct_lc_mixed
SELECT
    number,
    toLowCardinality(toString(intDiv(number, 10))),
    number % 5
FROM numbers(100000);

SELECT
    (SELECT count() FROM (SELECT DISTINCT k1, k2 FROM distinct_lc_mixed))
  - (SELECT uniqExact(k1, k2) FROM distinct_lc_mixed);

SELECT
    (SELECT count() FROM (SELECT DISTINCT k1 FROM distinct_lc_mixed))
  - (SELECT uniqExact(k1) FROM distinct_lc_mixed);

DROP TABLE IF EXISTS distinct_lc_all_same;
CREATE TABLE distinct_lc_all_same
(
    id UInt32,
    s String,
    lc LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO distinct_lc_all_same
SELECT
    number,
    'x',
    toLowCardinality('x')
FROM numbers(100000);

SELECT
    (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_all_same))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_all_same));

SELECT
    (SELECT arraySort(groupArray(s)) FROM (SELECT DISTINCT s FROM distinct_lc_all_same))
    =
    (SELECT arraySort(groupArray(CAST(lc AS String))) FROM (SELECT DISTINCT lc FROM distinct_lc_all_same));

DROP TABLE IF EXISTS distinct_lc_sparse_nulls;
CREATE TABLE distinct_lc_sparse_nulls
(
    id UInt32,
    s Nullable(String),
    lc LowCardinality(Nullable(String))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO distinct_lc_sparse_nulls
SELECT
    number,
    if(number IN (0, 1, 2, 3, 4), NULL, toString(number % 1000)),
    toLowCardinality(if(number IN (0, 1, 2, 3, 4), NULL, toString(number % 1000)))
FROM numbers(100000);

SELECT
    (SELECT count() FROM (SELECT DISTINCT s FROM distinct_lc_sparse_nulls))
  - (SELECT count() FROM (SELECT DISTINCT lc FROM distinct_lc_sparse_nulls));

SELECT
    (SELECT arraySort(groupArray(s)) FROM (SELECT DISTINCT s FROM distinct_lc_sparse_nulls))
    =
    (SELECT arraySort(groupArray(lc)) FROM (SELECT DISTINCT lc FROM distinct_lc_sparse_nulls));
