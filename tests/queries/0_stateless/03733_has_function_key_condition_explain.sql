-- Tags: no-replicated-database, no-parallel-replicas, no-parallel, no-random-merge-tree-settings
-- add_minmax_index_for_numeric_columns=0: Different plan
-- EXPLAIN output may differ

-- { echoOn }

DROP TABLE IF EXISTS test_has_idx_simple;

CREATE TABLE test_has_idx_simple
(
    id UInt32,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1000, add_minmax_index_for_numeric_columns=0;

INSERT INTO test_has_idx_simple
SELECT number, toString(number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_simple
WHERE has([10, 50000, 90000], id);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_simple
WHERE has([10, 50000, 90000], toUInt64(id + 2));

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_simple
WHERE has([10, 50000, 90000, NULL, NULL], toUInt64(id + 2));

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_simple
WHERE toUInt64(id + 2) IN (10, 50000, 90000, NULL, NULL);

DROP TABLE IF EXISTS test_has_idx_tuple_col;

CREATE TABLE test_has_idx_tuple_col
(
    id UInt32,
    key_tuple Tuple(UInt32, UInt32),
    payload String
)
ENGINE = MergeTree
ORDER BY key_tuple
SETTINGS index_granularity = 1000;

INSERT INTO test_has_idx_tuple_col
SELECT number,
       (number, number % 10),
       toString(number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_tuple_col
WHERE has([(10, 0), (50000, 0)], key_tuple);

DROP TABLE IF EXISTS test_has_idx_tuple_col_nullable_elements;

CREATE TABLE test_has_idx_tuple_col_nullable_elements
(
    id UInt32,
    key_tuple Tuple(Nullable(UInt32), Nullable(UInt32)),
    payload String
)
ENGINE = MergeTree
ORDER BY key_tuple
SETTINGS index_granularity = 1000, allow_nullable_key = 1;

INSERT INTO test_has_idx_tuple_col_nullable_elements
SELECT 
    number,
    tuple(
        number,
        NULL
    ),
    toString(number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_tuple_col_nullable_elements
WHERE has([(10, 0), (50000, 0), (0, NULL), (NULL, 10), (NULL, 20)], key_tuple);

DROP TABLE IF EXISTS test_has_idx_array_col;

CREATE TABLE test_has_idx_array_col
(
    id UInt32,
    arr_key Array(UInt32),
    payload String
)
ENGINE = MergeTree
ORDER BY arr_key
SETTINGS index_granularity = 1000;

INSERT INTO test_has_idx_array_col
SELECT number,
       [number, number + 1],
       toString(number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_array_col
WHERE has([[10, 11], [50000, 50001]], arr_key);

DROP TABLE IF EXISTS test_has_idx_tuple_two_cols;

CREATE TABLE test_has_idx_tuple_two_cols
(
    k1 UInt32,
    k2 UInt32,
    payload String
)
ENGINE = MergeTree
ORDER BY (k1, k2)
SETTINGS index_granularity = 1000, add_minmax_index_for_numeric_columns=0;

INSERT INTO test_has_idx_tuple_two_cols
SELECT number,
       number % 10,
       toString(number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_tuple_two_cols
WHERE has([(10, 0), (50000, 0)], (k1, k2));

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_tuple_two_cols
WHERE has([(10, 0), (50000, 0), (NULL, NULL)], (k1, k2));

DROP TABLE IF EXISTS test_has_idx_lowcard;

CREATE TABLE test_has_idx_lowcard
(
    id UInt32,
    key_lc LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY key_lc
SETTINGS index_granularity = 1000;

INSERT INTO test_has_idx_lowcard
SELECT number,
       toString((number % 100) + 1000000)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_lowcard
WHERE has(['1000010', '1000042', '1000077'], key_lc);

DROP TABLE IF EXISTS test_has_idx_nullable;

CREATE TABLE test_has_idx_nullable
(
    id UInt32,
    key_nullable Nullable(UInt32)
)
ENGINE = MergeTree
ORDER BY key_nullable
SETTINGS index_granularity = 1000, allow_nullable_key = 1;

INSERT INTO test_has_idx_nullable
SELECT number,
       if(number % 10 = 0, NULL, number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_nullable
WHERE has([11, 50000, 90000], key_nullable);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_nullable
WHERE has([11, 50000, 90000, NULL], key_nullable);

DROP TABLE IF EXISTS test_has_idx_func_key;

CREATE TABLE test_has_idx_func_key
(
    ts DateTime,
    payload String
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 1000;

INSERT INTO test_has_idx_func_key
SELECT
    toDate('2020-01-01') + number,
    toString(number)
FROM numbers(100000);

EXPLAIN indexes = 1
SELECT count()
FROM test_has_idx_func_key
WHERE has([toDate('2020-01-01'), toDate('2020-01-02'), toDate('2020-01-03')], toDate(ts));

DROP TABLE IF EXISTS t1;

CREATE TABLE t1
(
    c1 UInt64
)
ENGINE = MergeTree()
ORDER BY (c1);

INSERT INTO t1 VALUES (1);

EXPLAIN indexes = 1
SELECT count()
FROM t1
WHERE has([], c1);
