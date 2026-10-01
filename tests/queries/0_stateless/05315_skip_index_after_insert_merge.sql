-- Skip indexes of a part written by INSERT with optimize_on_insert describe the merged rows.

DROP TABLE IF EXISTS t_summing_length;
DROP TABLE IF EXISTS t_summing_modulo;
DROP TABLE IF EXISTS t_summing_map_keys;
DROP TABLE IF EXISTS t_coalescing;
DROP TABLE IF EXISTS t_aggregating;
DROP TABLE IF EXISTS t_summing_zero;

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

-- Rows whose summed columns total zero are removed; a skip index input must not keep them.
CREATE TABLE t_summing_zero (id UInt64, name String, v Int64, INDEX il length(name) TYPE minmax GRANULARITY 1)
ENGINE = SummingMergeTree ORDER BY id SETTINGS index_granularity = 100;
INSERT INTO t_summing_zero SELECT number % 1000, 'abcde', if(number < 1000, 1, -1) FROM numbers(2000)
SETTINGS optimize_on_insert = 1, max_insert_threads = 1, max_block_size = 65536;
SELECT 'summing rows summed to zero', count() FROM t_summing_zero;

DROP TABLE t_summing_length;
DROP TABLE t_summing_modulo;
DROP TABLE t_summing_map_keys;
DROP TABLE t_coalescing;
DROP TABLE t_aggregating;
DROP TABLE t_summing_zero;
