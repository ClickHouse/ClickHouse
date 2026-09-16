-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- A read served from the columns cache has to produce columns with the structure the part gives
-- them, not the structure of a freshly created column: the dynamic paths of a `JSON` column and
-- the variants of a `Dynamic` column come from the prefix of the part, and a column stored as a
-- single value per part (the codebook of a `Quantized` codec) holds that value. The
-- distinct-paths subcolumn of a `JSON` column is not a function of the rows it is read for, so it
-- is never cached: reading it with the cache gives what reading it without the cache gives.

SET max_threads = 1;
SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_cc_json;

CREATE TABLE t_cc_json (id UInt64, json JSON(a0 String, max_dynamic_paths = 4), d Dynamic(max_types = 3), vec Array(Float32) CODEC(Quantized('rabitq', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_cc_json SELECT
    number,
    format('{{"a0":"s{0}","a{1}":{0},"b{2}":"x","c{3}":[{0}]}}', number, number % 3 + 1, number % 5, number % 7),
    multiIf(number % 5 = 0, number::Dynamic, number % 5 = 1, toString(number)::Dynamic, number % 5 = 2, toDate(number)::Dynamic, number % 5 = 3, [number]::Dynamic, (number * 1.5)::Dynamic),
    arrayMap(i -> toFloat32(i + number % 7), range(64))
FROM numbers(50000);

SYSTEM DROP COLUMNS CACHE;

-- Without the cache, with the cache while it is filled, and with the cache warm: the same results.
SELECT 'dynamic paths', arraySort(groupUniqArray(JSONDynamicPaths(json))) FROM t_cc_json SETTINGS use_columns_cache = 0;
SELECT 'dynamic paths', arraySort(groupUniqArray(JSONDynamicPaths(json))) FROM t_cc_json SETTINGS use_columns_cache = 1;
SELECT 'dynamic paths', arraySort(groupUniqArray(JSONDynamicPaths(json))) FROM t_cc_json SETTINGS use_columns_cache = 1;

SELECT 'shared data paths', arraySort(groupUniqArray(JSONSharedDataPaths(json))) FROM t_cc_json SETTINGS use_columns_cache = 0;
SELECT 'shared data paths', arraySort(groupUniqArray(JSONSharedDataPaths(json))) FROM t_cc_json SETTINGS use_columns_cache = 1;
SELECT 'shared data paths', arraySort(groupUniqArray(JSONSharedDataPaths(json))) FROM t_cc_json SETTINGS use_columns_cache = 1;

SELECT 'json values', sum(cityHash64(toString(json))), count() FROM t_cc_json SETTINGS use_columns_cache = 0;
SELECT 'json values', sum(cityHash64(toString(json))), count() FROM t_cc_json SETTINGS use_columns_cache = 1;
SELECT 'json values', sum(cityHash64(toString(json))), count() FROM t_cc_json SETTINGS use_columns_cache = 1;

SELECT 'dynamic types', dynamicType(d) AS t, count() FROM t_cc_json GROUP BY t ORDER BY t SETTINGS use_columns_cache = 0;
SELECT 'dynamic types', dynamicType(d) AS t, count() FROM t_cc_json GROUP BY t ORDER BY t SETTINGS use_columns_cache = 1;
SELECT 'dynamic types', dynamicType(d) AS t, count() FROM t_cc_json GROUP BY t ORDER BY t SETTINGS use_columns_cache = 1;

SELECT 'dynamic values', sum(cityHash64(toString(d))) FROM t_cc_json SETTINGS use_columns_cache = 0;
SELECT 'dynamic values', sum(cityHash64(toString(d))) FROM t_cc_json SETTINGS use_columns_cache = 1;
SELECT 'dynamic values', sum(cityHash64(toString(d))) FROM t_cc_json SETTINGS use_columns_cache = 1;

SELECT 'quantized vectors', sum(cityHash64(toString(vec))) FROM t_cc_json SETTINGS use_columns_cache = 0;
SELECT 'quantized vectors', sum(cityHash64(toString(vec))) FROM t_cc_json SETTINGS use_columns_cache = 1;
SELECT 'quantized vectors', sum(cityHash64(toString(vec))) FROM t_cc_json SETTINGS use_columns_cache = 1;

SELECT 'distinct paths', distinctJSONPaths(json) FROM t_cc_json SETTINGS use_columns_cache = 0;
SELECT 'distinct paths', distinctJSONPaths(json) FROM t_cc_json SETTINGS use_columns_cache = 1;
SELECT 'distinct paths', distinctJSONPaths(json) FROM t_cc_json SETTINGS use_columns_cache = 1;

-- The distinct-paths subcolumn is never cached, the other columns are.
SELECT column, count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_json' GROUP BY column ORDER BY column;

DROP TABLE t_cc_json;
