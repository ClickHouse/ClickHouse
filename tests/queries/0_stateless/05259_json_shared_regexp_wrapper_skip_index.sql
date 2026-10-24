-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

-- A rules-only ALTER of JSON inside Array or Nullable keeps skip indexes of old parts, as for a bare JSON column.
DROP TABLE IF EXISTS wrapper_index_05259;

CREATE TABLE wrapper_index_05259
(
    id UInt64,
    arr Array(JSON(max_dynamic_paths=2, SHARED REGEXP '^zzz$')),
    nul Nullable(JSON(max_dynamic_paths=2, SHARED REGEXP '^zzz$')),
    INDEX arr_len length(arr) TYPE minmax GRANULARITY 1,
    INDEX nul_text toString(nul) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO wrapper_index_05259
SELECT
    number,
    if(number = 7, ['{"a":1}', '{"a":2}', '{"a":3}'], ['{"a":0}']),
    if(number = 7, '{"needle":1}', toJSONString(map('a', number)))
FROM numbers(24);

ALTER TABLE wrapper_index_05259
    MODIFY COLUMN arr Array(JSON(max_dynamic_paths=2, SHARED REGEXP '^yyy$')),
    MODIFY COLUMN nul Nullable(JSON(max_dynamic_paths=2, SHARED REGEXP '^yyy$'));

SELECT 'mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 'wrapper_index_05259';

SELECT 'array', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM wrapper_index_05259 WHERE length(arr) = 3 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Granules%';
SELECT 'nullable', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM wrapper_index_05259 WHERE toString(nul) = '{"needle":1}' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Granules%';

SELECT 'values', id FROM wrapper_index_05259 WHERE length(arr) = 3;
SELECT 'values', id FROM wrapper_index_05259 WHERE toString(nul) = '{"needle":1}';

DROP TABLE wrapper_index_05259;

-- A timezone change of a typed path in the same ALTER still drops the old granules.
DROP TABLE IF EXISTS wrapper_timezone_05259;

CREATE TABLE wrapper_timezone_05259
(
    id UInt64,
    nul Nullable(JSON(a DateTime('UTC'), SHARED REGEXP '^zzz$')),
    INDEX nul_text toString(nul) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO wrapper_timezone_05259 SELECT number, toJSONString(map('a', toString(toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600))) FROM numbers(24);

ALTER TABLE wrapper_timezone_05259 MODIFY COLUMN nul Nullable(JSON(a DateTime('Asia/Tokyo'), SHARED REGEXP '^yyy$'));

SELECT 'rules and timezone', id FROM wrapper_timezone_05259 WHERE toString(nul) = '{"a":"2020-01-01 09:00:00"}';

DROP TABLE wrapper_timezone_05259;
