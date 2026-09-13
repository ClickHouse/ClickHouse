-- `_headers` is appended in requested-virtual-column order, so both orders below must work.
SELECT mapContains(_headers, 'X-ClickHouse-Query-Id'), isNull(_time)
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

SELECT isNull(_time), mapContains(_headers, 'X-ClickHouse-Query-Id')
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

-- A cached row count must not be used when `_headers` is requested: the shortcut skips the data
-- GET whose response headers are the only source of the map. Both statements carry the same
-- settings so they share one schema-cache key.
SELECT count()
FROM url('http://127.0.0.1:8123/?query=select+5210&user=default', LineAsString, 's String')
SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1, schema_inference_cache_require_modification_time_for_url = 0;

-- The pair above is only meaningful if the warm-up really cached a row count for this URI, so
-- assert the entry exists; otherwise the statement below could pass without any shortcut to decline.
SELECT max(number_of_rows) FROM system.schema_inference_cache
WHERE storage = 'URL' AND source LIKE '%select+5210%';

SELECT min(mapContains(_headers, 'X-ClickHouse-Query-Id'))
FROM url('http://127.0.0.1:8123/?query=select+5210&user=default', LineAsString, 's String')
SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1, schema_inference_cache_require_modification_time_for_url = 0;
