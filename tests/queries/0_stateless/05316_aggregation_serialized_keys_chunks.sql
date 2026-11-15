-- The keys of the `prealloc_serialized` aggregation method are serialized in chunks of rows that fit in the CPU cache.
-- The result must not depend on the size of the blocks, on the length of the keys (a single long key makes a chunk on its own)
-- and on the order in which the rows are accessed (the adaptive aggregator reads the keys of the staged rows out of order).

DROP TABLE IF EXISTS t_serialized_keys;
CREATE TABLE t_serialized_keys (k UInt64, s String, ns Nullable(String)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_serialized_keys SELECT number % 1000, toString(number % 777), if(number % 5 = 0, NULL, repeat('x', number % 300)) FROM numbers(300000);
INSERT INTO t_serialized_keys SELECT number, repeat('y', 100000 + number), repeat('z', 50000 + number) FROM numbers(30);

SELECT 'not nullable';
SELECT count(), sum(c), sum(cityHash64(k, s)) FROM (SELECT k, s, count() AS c FROM t_serialized_keys GROUP BY k, s) SETTINGS max_block_size = 65409, enable_adaptive_aggregator = 0;
SELECT count(), sum(c), sum(cityHash64(k, s)) FROM (SELECT k, s, count() AS c FROM t_serialized_keys GROUP BY k, s) SETTINGS max_block_size = 1000, enable_adaptive_aggregator = 0;
SELECT count(), sum(c), sum(cityHash64(k, s)) FROM (SELECT k, s, count() AS c FROM t_serialized_keys GROUP BY k, s) SETTINGS max_block_size = 65409, max_threads = 4, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 100;

SELECT 'nullable';
SELECT count(), sum(c), sum(cityHash64(k, ns)) FROM (SELECT k, ns, count() AS c FROM t_serialized_keys GROUP BY k, ns) SETTINGS max_block_size = 65409, enable_adaptive_aggregator = 0;
SELECT count(), sum(c), sum(cityHash64(k, ns)) FROM (SELECT k, ns, count() AS c FROM t_serialized_keys GROUP BY k, ns) SETTINGS max_block_size = 1000, enable_adaptive_aggregator = 0;
SELECT count(), sum(c), sum(cityHash64(k, ns)) FROM (SELECT k, ns, count() AS c FROM t_serialized_keys GROUP BY k, ns) SETTINGS max_block_size = 65409, max_threads = 4, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 100;

SELECT 'with aggregate functions';
SELECT count(), sum(m), sum(cityHash64(k, s)) FROM (SELECT k, s, max(length(ns)) AS m FROM t_serialized_keys GROUP BY k, s) SETTINGS max_block_size = 65409, enable_adaptive_aggregator = 0;
SELECT count(), sum(m), sum(cityHash64(k, s)) FROM (SELECT k, s, max(length(ns)) AS m FROM t_serialized_keys GROUP BY k, s) SETTINGS max_block_size = 65409, max_threads = 4, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 100;

DROP TABLE t_serialized_keys;
