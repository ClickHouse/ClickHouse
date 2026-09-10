-- Tags: shard

-- GROUP BY over a single String key must produce identical results with the packed
-- (`key_packed_string`) and the legacy (`key_string`) aggregation methods,
-- one-level, two-level, external and distributed two-level alike.

DROP TABLE IF EXISTS t_04649;
CREATE TABLE t_04649 (s String) ENGINE = MergeTree ORDER BY tuple();

-- Key lengths 2..62 bytes cover the PackedStringRef inline (<= 11 bytes), medium and large layouts.
INSERT INTO t_04649 SELECT concat(substring(repeat('abcdefgh', 8), 1, number % 60), '_', toString(number % 97)) FROM numbers(100000);
INSERT INTO t_04649 SELECT '' FROM numbers(10);
-- Embedded and trailing zero bytes.
INSERT INTO t_04649 SELECT concat('k', char(0), 'v', char(0), toString(number % 7), char(0)) FROM numbers(1000);

SELECT 'String key';
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_04649 GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 1;
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_04649 GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 0;

SELECT 'String key, two-level';
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_04649 GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1;
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_04649 GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 0, group_by_two_level_threshold = 1;

SELECT 'String key, external aggregation';
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_04649 GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 1, max_bytes_before_external_group_by = 1;
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_04649 GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 0, max_bytes_before_external_group_by = 1;

-- Shards and the initiator must agree on two-level bucketing during the memory-efficient merge.
SELECT 'String key, distributed two-level';
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_04649) GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, distributed_aggregation_memory_efficient = 1, prefer_localhost_replica = 0;
SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_04649) GROUP BY s)
    SETTINGS enable_packed_string_keys_in_aggregation = 0, group_by_two_level_threshold = 1, distributed_aggregation_memory_efficient = 1, prefer_localhost_replica = 0;

-- Nullable and LowCardinality String keys never use the packed method; the setting must not affect them.
SELECT 'Nullable(String) key';
SELECT count(), sum(c), sum(cityHash64(coalesce(n, '<NULL>'), c)) FROM (SELECT nullIf(s, '') AS n, count() AS c FROM t_04649 GROUP BY n)
    SETTINGS enable_packed_string_keys_in_aggregation = 1;
SELECT count(), sum(c), sum(cityHash64(coalesce(n, '<NULL>'), c)) FROM (SELECT nullIf(s, '') AS n, count() AS c FROM t_04649 GROUP BY n)
    SETTINGS enable_packed_string_keys_in_aggregation = 0;

SELECT 'LowCardinality(String) key';
SELECT count(), sum(c), sum(cityHash64(lc, c)) FROM (SELECT toLowCardinality(s) AS lc, count() AS c FROM t_04649 GROUP BY lc)
    SETTINGS enable_packed_string_keys_in_aggregation = 1;
SELECT count(), sum(c), sum(cityHash64(lc, c)) FROM (SELECT toLowCardinality(s) AS lc, count() AS c FROM t_04649 GROUP BY lc)
    SETTINGS enable_packed_string_keys_in_aggregation = 0;

DROP TABLE t_04649;
