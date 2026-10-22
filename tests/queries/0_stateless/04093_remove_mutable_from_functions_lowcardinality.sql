-- Test that functions refactored to use IFunctionOverloadResolver (PR #100243)
-- work correctly with LowCardinality arguments.
-- The key concern is that buildImpl receives original arguments (with LowCardinality
-- wrappers) while getReturnTypeImpl sees arguments with LowCardinality already stripped
-- by the framework. If buildImpl doesn't handle this, it can make wrong decisions.

SET allow_suspicious_low_cardinality_types = 1;

-- toStartOfInterval: basic (Default overload) with LowCardinality first argument
SELECT 'toStartOfInterval with LC';

DROP TABLE IF EXISTS test_lc_interval;
CREATE TABLE test_lc_interval (dt LowCardinality(DateTime)) ENGINE = Memory;
INSERT INTO test_lc_interval VALUES ('2024-01-15 14:30:00'), ('2024-01-15 15:45:00');

SELECT toStartOfInterval(dt, INTERVAL 1 HOUR) FROM test_lc_interval ORDER BY dt;
SELECT toStartOfInterval(dt, INTERVAL 1 DAY) FROM test_lc_interval ORDER BY dt;
SELECT toStartOfInterval(dt, INTERVAL 1 MONTH) FROM test_lc_interval ORDER BY dt;

-- toStartOfInterval: Origin overload with LowCardinality origin argument
-- buildImpl checks isDateOrDate32OrDateTimeOrDateTime64(args[2].type) to decide overload
SELECT 'toStartOfInterval with LC origin';

SELECT toStartOfInterval(
    materialize(toDateTime('2024-01-15 14:45:00')),
    INTERVAL 1 HOUR,
    toLowCardinality(toDateTime('2024-01-15 00:00:00'))
);

SELECT toStartOfInterval(dt, INTERVAL 1 HOUR, toLowCardinality(toDateTime('2024-01-15 00:00:00'))) FROM test_lc_interval ORDER BY dt;

-- dateTrunc: LowCardinality DateTime as second argument
SELECT 'dateTrunc with LC';

SELECT dateTrunc('hour', dt) FROM test_lc_interval ORDER BY dt;
SELECT dateTrunc('day', dt) FROM test_lc_interval ORDER BY dt;
SELECT dateTrunc('month', dt) FROM test_lc_interval ORDER BY dt;

-- dateTrunc: LowCardinality constant string as first argument
-- buildImpl uses checkAndGetColumnConst<ColumnString> which may fail on LC
SELECT dateTrunc(toLowCardinality('hour'), toDateTime('2024-01-15 14:30:00'));

-- toInterval: LowCardinality numeric argument
SELECT 'toInterval with LC';

DROP TABLE IF EXISTS test_lc_to_interval;
CREATE TABLE test_lc_to_interval (x LowCardinality(Int32)) ENGINE = Memory;
INSERT INTO test_lc_to_interval VALUES (1), (5), (10);

SELECT toInterval(x, 'day') FROM test_lc_to_interval ORDER BY x;
SELECT toInterval(x, 'hour') FROM test_lc_to_interval ORDER BY x;

-- toInterval: LowCardinality constant string unit
SELECT toInterval(3, toLowCardinality('day'));

-- initializeAggregation: LowCardinality value arguments
-- buildImpl passes argument types to AggregateFunctionFactory which may not handle LC
SELECT 'initializeAggregation with LC';

DROP TABLE IF EXISTS test_lc_init_agg;
CREATE TABLE test_lc_init_agg (x LowCardinality(UInt32)) ENGINE = Memory;
INSERT INTO test_lc_init_agg VALUES (10), (20), (30);

SELECT initializeAggregation('sum', x) FROM test_lc_init_agg ORDER BY x;
SELECT initializeAggregation('max', x) FROM test_lc_init_agg ORDER BY x;
SELECT initializeAggregation('min', x) FROM test_lc_init_agg ORDER BY x;

-- initializeAggregation: LowCardinality String
DROP TABLE IF EXISTS test_lc_init_agg_str;
CREATE TABLE test_lc_init_agg_str (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO test_lc_init_agg_str VALUES ('hello'), ('world');

SELECT initializeAggregation('any', s) FROM test_lc_init_agg_str ORDER BY s;

-- arrayReduce: explicitly disables LC handling, so should work as-is
SELECT 'arrayReduce with LC';
SELECT arrayReduce('sum', [toLowCardinality(toUInt32(1)), toLowCardinality(toUInt32(2)), toLowCardinality(toUInt32(3))]);

-- arrayReduceInRanges: with LowCardinality array elements
SELECT 'arrayReduceInRanges with LC';

DROP TABLE IF EXISTS test_lc_arr_reduce;
CREATE TABLE test_lc_arr_reduce (arr Array(LowCardinality(UInt32))) ENGINE = Memory;
INSERT INTO test_lc_arr_reduce VALUES ([1, 2, 3, 4, 5]);

SELECT arrayReduceInRanges('sum', [(1, 3), (2, 4)], arr) FROM test_lc_arr_reduce;

-- transform: LowCardinality first argument
SELECT 'transform with LC';

DROP TABLE IF EXISTS test_lc_transform;
CREATE TABLE test_lc_transform (x LowCardinality(UInt32)) ENGINE = Memory;
INSERT INTO test_lc_transform VALUES (1), (2), (3), (4);

SELECT transform(x, [1, 2, 3], [10, 20, 30]) FROM test_lc_transform ORDER BY x;
SELECT transform(x, [1, 2, 3], [10, 20, 30], 0) FROM test_lc_transform ORDER BY x;

-- transform: LowCardinality String
DROP TABLE IF EXISTS test_lc_transform_str;
CREATE TABLE test_lc_transform_str (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO test_lc_transform_str VALUES ('a'), ('b'), ('c'), ('d');

SELECT transform(s, ['a', 'b', 'c'], ['x', 'y', 'z']) FROM test_lc_transform_str ORDER BY s;
SELECT transform(s, ['a', 'b', 'c'], ['x', 'y', 'z'], '?') FROM test_lc_transform_str ORDER BY s;

-- Verify results match non-LowCardinality versions
SELECT 'verify equivalence';

SELECT
    toStartOfInterval(toDateTime('2024-01-15 14:30:00'), INTERVAL 1 HOUR)
    = toStartOfInterval(toLowCardinality(toDateTime('2024-01-15 14:30:00')), INTERVAL 1 HOUR);

SELECT
    dateTrunc('hour', toDateTime('2024-01-15 14:30:00'))
    = dateTrunc('hour', toLowCardinality(toDateTime('2024-01-15 14:30:00')));

SELECT
    initializeAggregation('sum', toUInt32(42))
    = initializeAggregation('sum', toLowCardinality(toUInt32(42)));

SELECT
    transform(1, [1, 2, 3], [10, 20, 30], 0)
    = transform(toLowCardinality(toUInt32(1)), [1, 2, 3], [10, 20, 30], toUInt32(0));

-- Cleanup
DROP TABLE IF EXISTS test_lc_interval;
DROP TABLE IF EXISTS test_lc_to_interval;
DROP TABLE IF EXISTS test_lc_init_agg;
DROP TABLE IF EXISTS test_lc_init_agg_str;
DROP TABLE IF EXISTS test_lc_arr_reduce;
DROP TABLE IF EXISTS test_lc_transform;
DROP TABLE IF EXISTS test_lc_transform_str;
