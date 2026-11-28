SET max_block_size = 100;

-- Basic correctness: same numeric type (NumericData path: value type == key type)
SELECT sumDistinctBy(k, k) FROM (SELECT number % 5 AS k FROM numbers_mt(10000));

-- Basic correctness: different numeric sizes, 1-byte value (NumericKeyData path, value_size=1)
SELECT sumDistinctBy(toUInt8(number % 5), toUInt32(number % 5)) FROM numbers_mt(10000);

-- Basic correctness: different numeric sizes, 2-byte value (NumericKeyData path, value_size=2)
SELECT sumDistinctBy(toUInt16(number % 5), toUInt32(number % 5)) FROM numbers_mt(10000);

-- Basic correctness: different numeric sizes, 8-byte value (NumericKeyData path, value_size=8)
SELECT sumDistinctBy(toUInt64(number % 5), toUInt32(number % 5)) FROM numbers_mt(10000);

-- Basic correctness: String key (GenericData path)
SELECT sumDistinctBy(number % 5, toString(number % 5)) FROM numbers_mt(10000);

-- GenericData path: Tuple key (composite), LCM(3,7)=21 distinct pairs
SELECT countDistinctBy(1, (number % 3, number % 7)) FROM numbers_mt(21000);

-- GenericData path: String value and String key
SELECT arraySort(groupArrayDistinctBy(toString(number % 5), toString(number % 5))) FROM numbers_mt(10000);

-- Multiple groups: confirm per-group deduplication
SELECT grp, sumDistinctBy(k, k)
FROM (SELECT number % 4 AS grp, number % 5 AS k FROM numbers_mt(10000))
GROUP BY grp
ORDER BY grp;

-- Multi-threading: NumericData path
SELECT sumDistinctBy(k, k)
FROM (SELECT number % 100 AS k FROM numbers_mt(1000000));

-- Multi-threading: NumericKeyData path
SELECT sumDistinctBy(toUInt16(number % 100), toUInt64(number % 100))
FROM numbers_mt(1000000);

-- Multi-threading: GenericData path (String key)
SELECT countDistinctBy(1, toString(number % 50)) FROM numbers_mt(1000000);

-- Multi-threading: GenericData path (Tuple key)
SELECT countDistinctBy(1, (number % 7, number % 11)) FROM numbers_mt(77000);

-- Error: too few arguments
SELECT sumDistinctBy(number) FROM numbers_mt(10000); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- AggregatingMergeTree: NumericData path (same types, UInt64 value + UInt64 key)
DROP TABLE IF EXISTS test_distinct_by_num;
CREATE TABLE test_distinct_by_num
(
    grp UInt32,
    s   AggregateFunction(sumDistinctBy, UInt64, UInt64)
) ENGINE = AggregatingMergeTree() ORDER BY grp;

INSERT INTO test_distinct_by_num
SELECT grp, sumDistinctByState(k, k)
FROM (SELECT number % 3 AS grp, toUInt64(number % 5) AS k FROM numbers_mt(15000))
GROUP BY grp;

SELECT grp, sumDistinctByMerge(s) FROM test_distinct_by_num GROUP BY grp ORDER BY grp;

INSERT INTO test_distinct_by_num
SELECT grp, sumDistinctByState(k, k)
FROM (SELECT number % 3 AS grp, toUInt64((number % 5) + 5) AS k FROM numbers_mt(15000))
GROUP BY grp;

OPTIMIZE TABLE test_distinct_by_num FINAL;

SELECT grp, sumDistinctByMerge(s) FROM test_distinct_by_num GROUP BY grp ORDER BY grp;

DROP TABLE test_distinct_by_num;

-- AggregatingMergeTree: NumericKeyData path (UInt8 value + UInt32 key, value_size=1)
DROP TABLE IF EXISTS test_distinct_by_keydata;
CREATE TABLE test_distinct_by_keydata
(
    grp UInt32,
    s   AggregateFunction(sumDistinctBy, UInt8, UInt32)
) ENGINE = AggregatingMergeTree() ORDER BY grp;

INSERT INTO test_distinct_by_keydata
SELECT grp, sumDistinctByState(toUInt8(k), toUInt32(k))
FROM (SELECT number % 3 AS grp, number % 5 AS k FROM numbers_mt(15000))
GROUP BY grp;

SELECT grp, sumDistinctByMerge(s) FROM test_distinct_by_keydata GROUP BY grp ORDER BY grp;

INSERT INTO test_distinct_by_keydata
SELECT grp, sumDistinctByState(toUInt8(k), toUInt32(k))
FROM (SELECT number % 3 AS grp, (number % 5) + 5 AS k FROM numbers_mt(15000))
GROUP BY grp;

OPTIMIZE TABLE test_distinct_by_keydata FINAL;

SELECT grp, sumDistinctByMerge(s) FROM test_distinct_by_keydata GROUP BY grp ORDER BY grp;

DROP TABLE test_distinct_by_keydata;

-- AggregatingMergeTree: GenericData path (UInt64 value + String key)
DROP TABLE IF EXISTS test_distinct_by_str;
CREATE TABLE test_distinct_by_str
(
    grp UInt32,
    s   AggregateFunction(sumDistinctBy, UInt64, String)
) ENGINE = AggregatingMergeTree() ORDER BY grp;

INSERT INTO test_distinct_by_str
SELECT grp, sumDistinctByState(k, toString(k))
FROM (SELECT number % 3 AS grp, toUInt64(number % 5) AS k FROM numbers_mt(15000))
GROUP BY grp;

SELECT grp, sumDistinctByMerge(s) FROM test_distinct_by_str GROUP BY grp ORDER BY grp;

INSERT INTO test_distinct_by_str
SELECT grp, sumDistinctByState(k, toString(k))
FROM (SELECT number % 3 AS grp, toUInt64((number % 5) + 5) AS k FROM numbers_mt(15000))
GROUP BY grp;

OPTIMIZE TABLE test_distinct_by_str FINAL;

SELECT grp, sumDistinctByMerge(s) FROM test_distinct_by_str GROUP BY grp ORDER BY grp;

DROP TABLE test_distinct_by_str;
