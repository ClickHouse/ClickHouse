-- A `JSONAllValues` index stores the text of each value as the value is stored, so a constant compared
-- against a JSON subcolumn has to be coerced to the key expression type before it is turned into an
-- index probe. Every query below must return the same count with and without the skip index.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_json_all_values_coercion;

CREATE TABLE t_json_all_values_coercion
(
    data JSON(ip IPv4, ips Array(IPv4), missing Int64, ts DateTime, x UInt16),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

-- Two parts of one granule each, so a wrongly built probe drops the granule holding the match.
INSERT INTO t_json_all_values_coercion
SELECT '{"ip":"1.2.3.4","ips":["1.2.3.4"],"ts":"2026-01-01 00:00:00","x":256,"dynamic_x":256}'
FROM numbers(4);
INSERT INTO t_json_all_values_coercion
SELECT '{"ip":"8.8.8.8","ips":["8.8.8.8"],"ts":"2020-05-05 10:00:00","x":1,"dynamic_x":1}'
FROM numbers(4);

-- The index holds "1.2.3.4"; probing the literal as written would look for "16909060".
SELECT 'ipv4 from uint32 index  ', count() FROM t_json_all_values_coercion WHERE data.ip = toUInt32(16909060);
SELECT 'ipv4 from uint32 noindex', count() FROM t_json_all_values_coercion WHERE data.ip = toUInt32(16909060) SETTINGS use_skip_indexes = 0;

-- The index holds "2026-01-01 00:00:00"; probing the literal as written would look for "2026-01-01".
SELECT 'datetime from date index  ', count() FROM t_json_all_values_coercion WHERE data.ts = toDate('2026-01-01');
SELECT 'datetime from date noindex', count() FROM t_json_all_values_coercion WHERE data.ts = toDate('2026-01-01') SETTINGS use_skip_indexes = 0;

-- Coercion also applies recursively to whole arrays and to the searched element of `has`.
SELECT 'ipv4 array index  ', count() FROM t_json_all_values_coercion WHERE data.ips = [toUInt32(16909060)];
SELECT 'ipv4 array noindex', count() FROM t_json_all_values_coercion WHERE data.ips = [toUInt32(16909060)] SETTINGS use_skip_indexes = 0;

SELECT 'ipv4 has index  ', count() FROM t_json_all_values_coercion WHERE has(data.ips, toUInt32(16909060));
SELECT 'ipv4 has noindex', count() FROM t_json_all_values_coercion WHERE has(data.ips, toUInt32(16909060)) SETTINGS use_skip_indexes = 0;

-- An absent path reads as the key expression type's default value, which the index never recorded,
-- so a predicate that the default value satisfies matches every row and must not prune.
SELECT 'absent int default index  ', count() FROM t_json_all_values_coercion WHERE data.absent::Int64 = 0;
SELECT 'absent int default noindex', count() FROM t_json_all_values_coercion WHERE data.absent::Int64 = 0 SETTINGS use_skip_indexes = 0;

SELECT 'missing typed default index  ', count() FROM t_json_all_values_coercion WHERE data.missing = 0;
SELECT 'missing typed default noindex', count() FROM t_json_all_values_coercion WHERE data.missing = 0 SETTINGS use_skip_indexes = 0;

SELECT 'absent datetime default index  ', count() FROM t_json_all_values_coercion WHERE data.absent::DateTime = toDateTime(0);
SELECT 'absent datetime default noindex', count() FROM t_json_all_values_coercion WHERE data.absent::DateTime = toDateTime(0) SETTINGS use_skip_indexes = 0;

-- Non-identity casts are not matched to the value index, regardless of the comparison value.
SELECT 'absent int non-default index  ', count() FROM t_json_all_values_coercion WHERE data.absent::Int64 = 7;
SELECT 'absent int non-default noindex', count() FROM t_json_all_values_coercion WHERE data.absent::Int64 = 7 SETTINGS use_skip_indexes = 0;

-- A narrowing cast can map several stored values to the same result. The index stores "256", not "0".
SELECT 'typed narrowing cast index  ', count() FROM t_json_all_values_coercion WHERE data.x::UInt8 = 0;
SELECT 'typed narrowing cast noindex', count() FROM t_json_all_values_coercion WHERE data.x::UInt8 = 0 SETTINGS use_skip_indexes = 0;

SELECT 'dynamic narrowing cast index  ', count() FROM t_json_all_values_coercion WHERE data.dynamic_x::UInt8 = 0;
SELECT 'dynamic narrowing cast noindex', count() FROM t_json_all_values_coercion WHERE data.dynamic_x::UInt8 = 0 SETTINGS use_skip_indexes = 0;

-- Values whose literal type already matches the key expression type keep working.
SELECT 'ipv4 from ipv4 index  ', count() FROM t_json_all_values_coercion WHERE data.ip = toIPv4('1.2.3.4');
SELECT 'ipv4 from ipv4 noindex', count() FROM t_json_all_values_coercion WHERE data.ip = toIPv4('1.2.3.4') SETTINGS use_skip_indexes = 0;

SELECT 'identity cast index  ', count() FROM t_json_all_values_coercion WHERE data.ip::IPv4 = toUInt32(16909060);
SELECT 'identity cast noindex', count() FROM t_json_all_values_coercion WHERE data.ip::IPv4 = toUInt32(16909060) SETTINGS use_skip_indexes = 0;

-- A dynamic path has no single stored type, so the literal's own type is still used for the probe.
INSERT INTO t_json_all_values_coercion
SELECT '{"ip":"9.9.9.9","ips":["9.9.9.9"],"ts":"2021-02-02 02:02:02","x":9,"dynamic_x":9,"tag":"needle"}'
FROM numbers(4);

SELECT 'dynamic string index  ', count() FROM t_json_all_values_coercion WHERE data.tag = 'needle';
SELECT 'dynamic string noindex', count() FROM t_json_all_values_coercion WHERE data.tag = 'needle' SETTINGS use_skip_indexes = 0;

SELECT 'string cast index  ', count() FROM t_json_all_values_coercion WHERE data.tag::String = 'needle';
SELECT 'string cast noindex', count() FROM t_json_all_values_coercion WHERE data.tag::String = 'needle' SETTINGS use_skip_indexes = 0;

-- The index must still prune, otherwise the queries above would agree only because the optimization
-- was disabled altogether.
SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_json_all_values_coercion WHERE data.ip = toUInt32(16909060)
) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Description:%' OR explain LIKE '%Granules:%';

SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_json_all_values_coercion WHERE data.ip::IPv4 = toUInt32(16909060)
) WHERE explain LIKE '%Granules:%';

SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_json_all_values_coercion WHERE data.tag = 'needle'
) WHERE explain LIKE '%Granules:%';

SELECT trim(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_json_all_values_coercion WHERE data.tag::String = 'needle'
) WHERE explain LIKE '%Granules:%';

SELECT 'narrowing cast indexes', count() FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_json_all_values_coercion WHERE data.x::UInt8 = 0
) WHERE explain LIKE '%Name: idx_values%';

-- The default-value restriction applies to equality on a missing path, not to `has`, because
-- `has` on a missing array is false for every searched element.
SELECT 'has default indexes', count() FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_json_all_values_coercion WHERE has(data.ips, toUInt32(0))
) WHERE explain LIKE '%Name: idx_values%';

DROP TABLE t_json_all_values_coercion;
