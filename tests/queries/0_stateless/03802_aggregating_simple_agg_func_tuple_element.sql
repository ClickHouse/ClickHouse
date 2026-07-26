SET optimize_throw_if_noop = 1;

-- Test 1: Basic SimpleAggregateFunction subcolumn test
SELECT '=== Test 1: Basic SimpleAggregateFunction subcolumn test ===';
DROP TABLE IF EXISTS test_simple_subcolumn;

CREATE TABLE test_simple_subcolumn (
    metrics Tuple(
        id UInt64,
        val SimpleAggregateFunction(sum, Double)
    )
) ENGINE = AggregatingMergeTree() ORDER BY metrics.id
SETTINGS allow_tuple_element_aggregation = 1;

-- First insert
INSERT INTO test_simple_subcolumn SELECT tuple(number, number) FROM numbers(10);

-- Second insert to test merge behavior
INSERT INTO test_simple_subcolumn SELECT tuple(number, number) FROM numbers(10);

-- Test with FINAL to verify immediate aggregation
SELECT 'Basic test - with FINAL (after two inserts):';
SELECT * FROM test_simple_subcolumn FINAL ORDER BY metrics.id;

-- Test OPTIMIZE to verify on-disk aggregation
OPTIMIZE TABLE test_simple_subcolumn FINAL;

SELECT 'Basic test - after OPTIMIZE:';
SELECT * FROM test_simple_subcolumn ORDER BY metrics.id;

DROP TABLE test_simple_subcolumn;


-- Test 2: Comprehensive SimpleAggregateFunction types in subcolumns
SELECT '=== Test 2: Comprehensive SimpleAggregateFunction types ===';
DROP TABLE IF EXISTS test_comprehensive_simple;

CREATE TABLE test_comprehensive_simple (
    id UInt64,
    agg_tuple Tuple(
        sum_val SimpleAggregateFunction(sum, UInt64),
        min_val SimpleAggregateFunction(min, UInt64),
        max_val SimpleAggregateFunction(max, UInt64),
        any_val SimpleAggregateFunction(any, String),
        any_last_val SimpleAggregateFunction(anyLast, String),
        nullable_str SimpleAggregateFunction(anyLast, Nullable(String)),
        nullable_str_respect_nulls SimpleAggregateFunction(anyLastRespectNulls, Nullable(String)),
        low_str SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
        ip_addr SimpleAggregateFunction(anyLast, IPv4),
        status SimpleAggregateFunction(groupBitOr, UInt32),
        bit_and SimpleAggregateFunction(groupBitAnd, UInt32),
        bit_xor SimpleAggregateFunction(groupBitXor, UInt32),
        arr SimpleAggregateFunction(groupArrayArray, Array(Int32)),
        uniq_arr SimpleAggregateFunction(groupUniqArrayArray, Array(Int32))
    )
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert first batch
INSERT INTO test_comprehensive_simple VALUES(
    1,
    tuple(
        100, 10, 50, 'first', 'first_last', '1', '1', '1', '192.168.1.1', 
        1, 15, 5, [1,2], [1,2]
    )
);

-- Insert second batch with same key (should trigger aggregation)
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        200, 5, 100, 'second', 'second_last', null, null, '2', '192.168.1.2', 
        2, 7, 3, [2,3,4], [2,3,4]
    )
);

-- Insert third batch
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        50, 15, 75, 'third', 'third_last', '3', '3', '3', '192.168.1.3', 
        4, 3, 7, [5], [5,6]
    )
);

-- Insert data for id=10 with long string (longer than MAX_SMALL_STRING_SIZE)
INSERT INTO test_comprehensive_simple VALUES(
    10, 
    tuple(
        1000, 100, 500, 'ten', 'ten_last', '10', null, '10', '10.0.0.1', 
        8, 255, 15, [], []
    )
);

INSERT INTO test_comprehensive_simple VALUES(
    10, 
    tuple(
        2000, 50, 1000, 'twenty', '2222',
        '2222',
        '10', '20', '10.0.0.2', 
        16, 127, 31, [], []
    )
);

-- Test immediate aggregation with FINAL
SELECT 'Comprehensive test - with FINAL:';
SELECT
    id,
    agg_tuple.sum_val,
    agg_tuple.min_val,
    agg_tuple.max_val,
    agg_tuple.any_val,
    agg_tuple.any_last_val,
    agg_tuple.nullable_str,
    agg_tuple.nullable_str_respect_nulls,
    agg_tuple.low_str,
    agg_tuple.ip_addr,
    agg_tuple.status,
    agg_tuple.bit_and,
    agg_tuple.bit_xor,
    agg_tuple.arr,
    arraySort(agg_tuple.uniq_arr) AS uniq_arr
FROM test_comprehensive_simple FINAL ORDER BY id;

-- Test on-disk aggregation with OPTIMIZE
OPTIMIZE TABLE test_comprehensive_simple FINAL;

SELECT 'Comprehensive test - after OPTIMIZE:';
SELECT
    id,
    agg_tuple.sum_val,
    agg_tuple.min_val,
    agg_tuple.max_val,
    agg_tuple.any_val,
    agg_tuple.any_last_val,
    agg_tuple.nullable_str,
    agg_tuple.nullable_str_respect_nulls,
    agg_tuple.low_str,
    agg_tuple.ip_addr,
    agg_tuple.status,
    agg_tuple.bit_and,
    agg_tuple.bit_xor,
    agg_tuple.arr,
    arraySort(agg_tuple.uniq_arr) AS uniq_arr
FROM test_comprehensive_simple ORDER BY id;

DROP TABLE test_comprehensive_simple;

-- Test 3: Tuple column before sorting key column (removeReplicatedFromSortingColumns bug)
SELECT '=== Test 3: Tuple column before sorting key ===';
DROP TABLE IF EXISTS test_tuple_before_sort_key;

CREATE TABLE test_tuple_before_sort_key (
    agg_data Tuple(
        sum_val SimpleAggregateFunction(sum, UInt64),
        max_val SimpleAggregateFunction(max, UInt64),
        any_last_val SimpleAggregateFunction(anyLast, String)
    ),
    id UInt64
) ENGINE = AggregatingMergeTree() ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

-- Insert first batch
INSERT INTO test_tuple_before_sort_key VALUES (tuple(100, 50, 'first'), 1);
INSERT INTO test_tuple_before_sort_key VALUES (tuple(200, 80, 'second'), 2);
INSERT INTO test_tuple_before_sort_key VALUES (tuple(300, 90, 'third'), 3);

-- Insert second batch with same keys to trigger aggregation
INSERT INTO test_tuple_before_sort_key VALUES (tuple(150, 60, 'updated_first'), 1);
INSERT INTO test_tuple_before_sort_key VALUES (tuple(250, 85, 'updated_second'), 2);
INSERT INTO test_tuple_before_sort_key VALUES (tuple(350, 95, 'updated_third'), 3);

-- Test with FINAL
SELECT 'Tuple before sort key - with FINAL:';
SELECT id, agg_data.sum_val, agg_data.max_val, agg_data.any_last_val
FROM test_tuple_before_sort_key FINAL ORDER BY id;

-- Test after OPTIMIZE
OPTIMIZE TABLE test_tuple_before_sort_key FINAL;

SELECT 'Tuple before sort key - after OPTIMIZE:';
SELECT id, agg_data.sum_val, agg_data.max_val, agg_data.any_last_val
FROM test_tuple_before_sort_key ORDER BY id;

DROP TABLE test_tuple_before_sort_key;
