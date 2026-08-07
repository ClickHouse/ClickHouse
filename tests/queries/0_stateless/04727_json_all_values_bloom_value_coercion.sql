-- A `JSONAllValues` index stores the text representation of each JSON value. A comparison constant
-- must be converted to the concrete JSON subcolumn type before its Bloom-filter probe is built.

DROP TABLE IF EXISTS json_values_bloom_coercion;

CREATE TABLE json_values_bloom_coercion
(
    data JSON(ip IPv4, ips Array(IPv4), ts DateTime, x UInt16),
    INDEX idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO json_values_bloom_coercion
SELECT '{"ip":"1.2.3.4","ips":["1.2.3.4"],"ts":"2026-01-01 00:00:00","x":256,"tag":"needle"}'
FROM numbers(4);
INSERT INTO json_values_bloom_coercion
SELECT '{"ip":"8.8.8.8","ips":["8.8.8.8"],"ts":"2020-05-05 10:00:00","x":1,"tag":"other"}'
FROM numbers(4);

SELECT 'bloom equals index', count() FROM json_values_bloom_coercion
WHERE data.ip = toUInt32(16909060) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'bloom in index', count() FROM json_values_bloom_coercion
WHERE data.ip IN (toUInt32(16909060)) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'bloom date index', count() FROM json_values_bloom_coercion
WHERE data.ts = toDate('2026-01-01') SETTINGS force_data_skipping_indices = 'idx';

SELECT 'bloom array index', count() FROM json_values_bloom_coercion
WHERE data.ips = [toUInt32(16909060)] SETTINGS force_data_skipping_indices = 'idx';

SELECT 'bloom identity cast index', count() FROM json_values_bloom_coercion
WHERE data.ip::IPv4 = toUInt32(16909060) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'bloom string cast index', count() FROM json_values_bloom_coercion
WHERE data.tag::String = 'needle' SETTINGS force_data_skipping_indices = 'idx';

-- A narrowing cast changes the indexed representation and cannot be inverted into a safe probe.
SELECT 'bloom narrowing cast result', count() FROM json_values_bloom_coercion WHERE data.x::UInt8 = 0;
SELECT 'bloom narrowing cast indexes', count() FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_values_bloom_coercion WHERE data.x::UInt8 = 0
)
WHERE explain LIKE '%Name: idx%';

DROP TABLE json_values_bloom_coercion;

CREATE TABLE json_values_bloom_coercion
(
    data JSON(ip IPv4, ips Array(IPv4), ts DateTime, x UInt16),
    INDEX idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO json_values_bloom_coercion
SELECT '{"ip":"1.2.3.4","ips":["1.2.3.4"],"ts":"2026-01-01 00:00:00","x":256,"tag":"needle"}'
FROM numbers(4);
INSERT INTO json_values_bloom_coercion
SELECT '{"ip":"8.8.8.8","ips":["8.8.8.8"],"ts":"2020-05-05 10:00:00","x":1,"tag":"other"}'
FROM numbers(4);

SELECT 'token equals index', count() FROM json_values_bloom_coercion
WHERE data.ip = toUInt32(16909060) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'token in index', count() FROM json_values_bloom_coercion
WHERE data.ip IN (toUInt32(16909060)) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'token has index', count() FROM json_values_bloom_coercion
WHERE has(data.ips, toUInt32(16909060)) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'token hasAny index', count() FROM json_values_bloom_coercion
WHERE hasAny(data.ips, [toUInt32(16909060)]) SETTINGS force_data_skipping_indices = 'idx';

SELECT 'token date index', count() FROM json_values_bloom_coercion
WHERE data.ts = toDate('2026-01-01') SETTINGS force_data_skipping_indices = 'idx';

SELECT 'token narrowing cast result', count() FROM json_values_bloom_coercion WHERE data.x::UInt8 = 0;
SELECT 'token narrowing cast indexes', count() FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_values_bloom_coercion WHERE data.x::UInt8 = 0
)
WHERE explain LIKE '%Name: idx%';

DROP TABLE json_values_bloom_coercion;

CREATE TABLE json_values_bloom_coercion
(
    data JSON(ip IPv4),
    INDEX idx JSONAllValues(data) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO json_values_bloom_coercion SELECT '{"ip":"1.2.3.4"}' FROM numbers(4);
INSERT INTO json_values_bloom_coercion SELECT '{"ip":"8.8.8.8"}' FROM numbers(4);

SELECT 'ngram equals index', count() FROM json_values_bloom_coercion
WHERE data.ip = toUInt32(16909060) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_values_bloom_coercion;

CREATE TABLE json_values_bloom_coercion
(
    data JSON(ip IPv4),
    INDEX idx JSONAllValues(data) TYPE sparse_grams(3, 100, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO json_values_bloom_coercion SELECT '{"ip":"1.2.3.4"}' FROM numbers(4);
INSERT INTO json_values_bloom_coercion SELECT '{"ip":"8.8.8.8"}' FROM numbers(4);

SELECT 'sparse equals index', count() FROM json_values_bloom_coercion
WHERE data.ip = toUInt32(16909060) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_values_bloom_coercion;
