-- Skip indexes of a part written by INSERT with optimize_on_insert describe the merged rows.

DROP TABLE IF EXISTS t_summing_length;
DROP TABLE IF EXISTS t_summing_modulo;
DROP TABLE IF EXISTS t_summing_map_keys;
DROP TABLE IF EXISTS t_coalescing;
DROP TABLE IF EXISTS t_aggregating;
DROP TABLE IF EXISTS t_graphite;
DROP TABLE IF EXISTS t_graphite_hour;
DROP TABLE IF EXISTS t_summing_zero;
DROP TABLE IF EXISTS t_replacing;

CREATE TABLE t_summing_length (id UInt64, name String, v UInt64, INDEX il length(name) TYPE minmax GRANULARITY 1)
ENGINE = SummingMergeTree ORDER BY id SETTINGS index_granularity = 100;
INSERT INTO t_summing_length SELECT number % 1000, 'abcde', 1 FROM numbers(3000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'summing length(name)',
    (SELECT count() FROM t_summing_length WHERE length(name) = 5 SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_summing_length WHERE length(name) = 5 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

CREATE TABLE t_summing_modulo (id UInt64, v UInt64, INDEX im v % 7 TYPE minmax GRANULARITY 1)
ENGINE = SummingMergeTree ORDER BY id SETTINGS index_granularity = 100;
INSERT INTO t_summing_modulo SELECT number % 1000, 5 FROM numbers(3000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'summing v % 7',
    (SELECT count() FROM t_summing_modulo WHERE v % 7 = 1 SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_summing_modulo WHERE v % 7 = 1 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

CREATE TABLE t_summing_map_keys
(
    id UInt64,
    statsMap Tuple(a Map(String, UInt64), b Map(UInt64, UInt64)),
    v UInt64,
    INDEX ia mapKeys(statsMap.a) TYPE bloom_filter GRANULARITY 1,
    INDEX ib mapKeys(statsMap.b) TYPE bloom_filter GRANULARITY 1
)
ENGINE = SummingMergeTree ORDER BY id SETTINGS index_granularity = 1000;
INSERT INTO t_summing_map_keys SELECT number % 10000,
    tuple(map(concat('k', toString(intDiv(number % 10000, 1000))), 1, 'common', 2), map(toUInt64(intDiv(number % 10000, 1000)), 10, 100, 20)), 1
FROM numbers(20000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'summing mapKeys',
    (SELECT count() FROM t_summing_map_keys WHERE has(mapKeys(statsMap.b), 3) SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_summing_map_keys WHERE has(mapKeys(statsMap.b), 3) SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

CREATE TABLE t_coalescing (id UInt64, a Nullable(UInt64), b Nullable(UInt64), INDEX ic a + b TYPE minmax GRANULARITY 1)
ENGINE = CoalescingMergeTree ORDER BY id SETTINGS index_granularity = 100;
INSERT INTO t_coalescing SELECT number % 1000, if(number < 1000, 1, NULL), if(number >= 1000, 10, NULL) FROM numbers(2000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'coalescing a + b',
    (SELECT count() FROM t_coalescing WHERE a + b = 11 SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_coalescing WHERE a + b = 11 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

CREATE TABLE t_aggregating (id UInt64, v SimpleAggregateFunction(sum, UInt64), INDEX ia v % 7 TYPE minmax GRANULARITY 1)
ENGINE = AggregatingMergeTree ORDER BY id SETTINGS index_granularity = 100;
INSERT INTO t_aggregating SELECT number % 1000, 5 FROM numbers(3000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'aggregating v % 7',
    (SELECT count() FROM t_aggregating WHERE v % 7 = 1 SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_aggregating WHERE v % 7 = 1 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

-- Three points per path in one 600 s rollup window one day ago, rolled up into one row with Value = 15.
CREATE TABLE t_graphite (Path String, Time DateTime('UTC'), Value Float64, Version UInt32, INDEX iv toUInt64(Value) % 7 TYPE minmax GRANULARITY 1)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time) SETTINGS index_granularity = 100;
INSERT INTO t_graphite SELECT concat('sum_', toString(number % 1000)),
    toDateTime(intDiv(toUInt32(now()) - 86400, 600) * 600 + intDiv(number, 1000) * 60, 'UTC'), 5, 1
FROM numbers(3000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'graphite toUInt64(Value) % 7',
    (SELECT count() FROM t_graphite WHERE toUInt64(Value) % 7 = 1 SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_graphite WHERE toUInt64(Value) % 7 = 1 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

-- Rolled up into a 6000 s window three days ago, so the stored Time is in an earlier hour than the inserted one.
-- The index expression is also a sorting key expression.
CREATE TABLE t_graphite_hour (Path String, Time DateTime('UTC'), Value Float64, Version UInt32, INDEX ih toStartOfHour(Time) TYPE minmax GRANULARITY 1)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, toStartOfHour(Time)) SETTINGS index_granularity = 100;
INSERT INTO t_graphite_hour SELECT concat('sum_', toString(number)), toDateTime(intDiv(toUInt32(now()) - 3 * 86400, 6000) * 6000 + 5940, 'UTC'), 1, 1
FROM numbers(1000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'graphite toStartOfHour(Time)',
    (SELECT count() FROM t_graphite_hour WHERE toStartOfHour(Time) = (SELECT any(toStartOfHour(Time)) FROM t_graphite_hour) SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_graphite_hour WHERE toStartOfHour(Time) = (SELECT any(toStartOfHour(Time)) FROM t_graphite_hour) SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0);

-- Rows whose summed columns total zero are removed; a skip index input must not keep them.
CREATE TABLE t_summing_zero (id UInt64, name String, v Int64, INDEX il length(name) TYPE minmax GRANULARITY 1)
ENGINE = SummingMergeTree ORDER BY id SETTINGS index_granularity = 100;
INSERT INTO t_summing_zero SELECT number % 1000, 'abcde', if(number < 1000, 1, -1) FROM numbers(2000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'summing rows summed to zero', count() FROM t_summing_zero;

-- A row-preserving engine, and an index whose expression is also a sorting key expression.
CREATE TABLE t_replacing (id UInt64, ts DateTime('UTC'), name String, INDEX ik toStartOfHour(ts) TYPE minmax GRANULARITY 1, INDEX il length(name) TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree ORDER BY (toStartOfHour(ts), id) SETTINGS index_granularity = 100;
INSERT INTO t_replacing SELECT number % 1000, toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 1000) * 60, 'abcde' FROM numbers(3000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'replacing',
    (SELECT count() FROM t_replacing WHERE length(name) = 5 SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0),
    (SELECT count() FROM t_replacing WHERE length(name) = 5 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0),
    (SELECT count() FROM t_replacing WHERE toStartOfHour(ts) = toDateTime('2024-01-01 05:00:00', 'UTC') SETTINGS use_skip_indexes = 1, use_query_condition_cache = 0);

DROP TABLE t_summing_length;
DROP TABLE t_summing_modulo;
DROP TABLE t_summing_map_keys;
DROP TABLE t_coalescing;
DROP TABLE t_aggregating;
DROP TABLE t_graphite;
DROP TABLE t_graphite_hour;
DROP TABLE t_summing_zero;
DROP TABLE t_replacing;
