-- A comparison with a `Dynamic`, `Variant` or `JSON` value converts the other operand to each stored type,
-- while a key transform such as `CAST(d, 'String')` renders each value from its own type. No transformed
-- constant stands for all the values the comparison matches, so the key must not prune such a comparison.
-- Each query returns the row count with the primary key and without it, and both must agree.
-- https://github.com/ClickHouse/ClickHouse/issues/124451

SET use_query_condition_cache = 0;
SET allow_suspicious_variant_types = 1;

SELECT '-- a String constant parsed into the stored type';

CREATE TABLE t_date (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_date SELECT toDate('2024-01-01') + number FROM numbers(10);
SELECT 'Date', count(), (SELECT count() FROM t_date WHERE d = '2024-1-5' SETTINGS use_primary_key = 0) FROM t_date WHERE d = '2024-1-5';

CREATE TABLE t_uint (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_uint SELECT number FROM numbers(100);
SELECT 'UInt64', count(), (SELECT count() FROM t_uint WHERE d = '042' SETTINGS use_primary_key = 0) FROM t_uint WHERE d = '042';
SELECT 'UInt64 has', count(), (SELECT count() FROM t_uint WHERE has(['042'], d) SETTINGS use_primary_key = 0) FROM t_uint WHERE has(['042'], d);

CREATE TABLE t_uuid (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_uuid SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba' || toString(number)) FROM numbers(10);
SELECT 'UUID', count(), (SELECT count() FROM t_uuid WHERE d = '61F0C404-5CB3-11E7-907B-A6006AD3DBA1' SETTINGS use_primary_key = 0)
FROM t_uuid WHERE d = '61F0C404-5CB3-11E7-907B-A6006AD3DBA1';

CREATE TABLE t_array (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_array SELECT [number, number + 1] FROM numbers(10);
SELECT 'Array', count(), (SELECT count() FROM t_array WHERE d = '[1, 2]' SETTINGS use_primary_key = 0) FROM t_array WHERE d = '[1, 2]';

SELECT '-- a constant of another type converted to the stored type';

CREATE TABLE t_datetime (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_datetime SELECT toDateTime(toDate('2024-01-01') + number, 'UTC') FROM numbers(10);
SELECT 'Date against DateTime', count(), (SELECT count() FROM t_datetime WHERE d = toDate('2024-01-05') SETTINGS use_primary_key = 0)
FROM t_datetime WHERE d = toDate('2024-01-05');
SELECT 'Dynamic against DateTime', count(), (SELECT count() FROM t_datetime WHERE d = toDate('2024-01-05')::Dynamic SETTINGS use_primary_key = 0)
FROM t_datetime WHERE d = toDate('2024-01-05')::Dynamic;

CREATE TABLE t_tostring (d Dynamic) ENGINE = MergeTree ORDER BY toString(d) SETTINGS index_granularity = 1;
INSERT INTO t_tostring SELECT toDateTime(toDate('2024-01-01') + number, 'UTC') FROM numbers(10);
SELECT 'Dynamic against DateTime, toString key', count(), (SELECT count() FROM t_tostring WHERE d = toDate('2024-01-05')::Dynamic SETTINGS use_primary_key = 0)
FROM t_tostring WHERE d = toDate('2024-01-05')::Dynamic;

CREATE TABLE t_uint8 (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_uint8 SELECT number::UInt8 FROM numbers(10);
SELECT 'Bool against UInt8', count(), (SELECT count() FROM t_uint8 WHERE d = true SETTINGS use_primary_key = 0) FROM t_uint8 WHERE d = true;

CREATE TABLE t_float (d Dynamic) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_float VALUES (-0.0), (-0.5), (0.0), (2.0);
SELECT 'signed zero', count(), (SELECT count() FROM t_float WHERE d = 0.0 SETTINGS use_primary_key = 0) FROM t_float WHERE d = 0.0;

SELECT '-- Variant and JSON';

CREATE TABLE t_variant_constant (d Variant(Date, DateTime('UTC'))) ENGINE = MergeTree ORDER BY d::String SETTINGS index_granularity = 1;
INSERT INTO t_variant_constant SELECT toDateTime('2024-01-05 00:00:00', 'UTC')::Variant(Date, DateTime('UTC'));
SELECT 'Variant constant', count(), (SELECT count() FROM t_variant_constant WHERE d = toDate('2024-01-05')::Variant(Date, DateTime('UTC')) SETTINGS use_primary_key = 0)
FROM t_variant_constant WHERE d = toDate('2024-01-05')::Variant(Date, DateTime('UTC'));

CREATE TABLE t_json (j JSON) ENGINE = MergeTree ORDER BY j.a::String SETTINGS index_granularity = 1;
INSERT INTO t_json SELECT toJSONString(map('a', toDate('2024-01-01') + number))::JSON FROM numbers(10);
SELECT 'JSON', count(), (SELECT count() FROM t_json WHERE j.a = '2024-1-5' SETTINGS use_primary_key = 0) FROM t_json WHERE j.a = '2024-1-5';

SELECT '-- the key is still used when the query compares the key expression itself';
SELECT 'explicit CAST', count() FROM t_date WHERE d::String = '2024-01-05' SETTINGS force_primary_key = 1, max_rows_to_read = 2;

SELECT '-- the other key columns are still used';
CREATE TABLE t_tuple (a UInt32, d Dynamic) ENGINE = MergeTree ORDER BY (a, d::String) SETTINGS index_granularity = 1;
INSERT INTO t_tuple SELECT number % 10, number FROM numbers(100);
-- The key on `a` selects 21 of 100 granules. The limit is looser because a read pool may read a boundary granule in two streams.
SELECT 'AND', count() FROM t_tuple WHERE (a = 1 AND d = 11) OR (a = 2 AND d = 22) SETTINGS force_primary_key = 1, max_rows_to_read = 50;
SELECT 'has', count() FROM t_tuple WHERE has([(1, 11), (2, 22)], (a, d)) SETTINGS force_primary_key = 1, max_rows_to_read = 50;

DROP TABLE t_date, t_uint, t_uuid, t_array, t_datetime, t_tostring, t_uint8, t_float, t_variant_constant, t_json, t_tuple;
