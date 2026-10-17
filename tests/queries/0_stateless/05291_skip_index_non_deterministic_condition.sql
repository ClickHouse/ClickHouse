-- Tags: no-fasttest
-- no-fasttest: the JSON case needs the JSON type.

-- A skip index must not skip granules because of a function that can return another value on each evaluation,
-- such as `rand`: the index computes the condition on the values it stores, not on the rows.
-- https://github.com/ClickHouse/ClickHouse/issues/89615

DROP TABLE IF EXISTS t_set;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_name;

-- One value of `id` per granule: a granule skipped by mistake loses all of its rows.
CREATE TABLE t_set (id UInt64, INDEX s_idx id TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_set SELECT intDiv(number, 8) % 4 FROM numbers(8000);

SELECT '-- set index, rows out of 8000 within 11 standard deviations of the expected count';
SELECT 'rand, bulk', count() BETWEEN 3500 AND 4500 FROM t_set WHERE rand() % 2 = 0 SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT 'rand, per granule', count() BETWEEN 3500 AND 4500 FROM t_set WHERE rand() % 2 = 0 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT 'rand in PREWHERE', count() BETWEEN 3500 AND 4500 FROM t_set PREWHERE rand() % 2 = 0;
SELECT 'rand of id', count() BETWEEN 3500 AND 4500 FROM t_set WHERE rand(id) % 2 = 0;
SELECT 'generateUUIDv4', count() BETWEEN 3500 AND 4500 FROM t_set WHERE toUInt128(generateUUIDv4()) % 2 = 0;
SELECT 'rand in a lambda', count() BETWEEN 1500 AND 2500 FROM t_set WHERE arrayExists(x -> x = rand() % 4, [id]);
SELECT 'rand in if', count() BETWEEN 4500 AND 5500 FROM t_set WHERE if(rand() % 2 = 0, id = 1, 1);
SELECT 'id = 1 OR rand', count() BETWEEN 4500 AND 5500 FROM t_set WHERE id = 1 OR rand() % 2 = 0;
SELECT 'id = 1 AND rand', count() BETWEEN 750 AND 1250 FROM t_set WHERE id = 1 AND rand() % 2 = 0;

SELECT '-- set index, is the index used';
SELECT 'rand', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set WHERE rand() % 2 = 0 SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: s_idx%';
SELECT 'rowNumberInBlock', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set WHERE rowNumberInBlock() % 2 = 0 SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: s_idx%';
SELECT 'control: id = 1', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set WHERE id = 1 SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: s_idx%';

-- A column named like the function does not stand in for it.
CREATE TABLE t_name (id UInt64, `rand()` UInt32, INDEX r_idx (id, `rand()`) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_name SELECT intDiv(number, 8) % 4, 1 + intDiv(number, 8) % 2 FROM numbers(8000);

SELECT '-- set index on a column named rand()';
SELECT 'rand', count() BETWEEN 3500 AND 4500 FROM t_name WHERE rand() % 2 = 0;
SELECT 'id < 10 AND rand', count() BETWEEN 3500 AND 4500 FROM t_name WHERE id < 10 AND rand() % 2 = 0;
SELECT 'rand, index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_name WHERE rand() % 2 = 0 SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: r_idx%';

-- Every second granule has the key 'k', the others only the key 'z'.
CREATE TABLE t_map (id UInt64, m Map(String, String), INDEX ki mapKeys(m) TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, text_index_posting_list_block_size = 1048576;
INSERT INTO t_map SELECT number, if(intDiv(number, 8) % 2 = 0, map('k', 'v'), map('z', 'w')) FROM numbers(8000);

SELECT '-- text index on map keys: 4000 rows have the key, about 40 of the others pass through rand';
SELECT 'map element', count() > 4000 FROM t_map WHERE if(m['k'] = 'v', 1, rand() % 100 = 0) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'map subcolumn', count() > 4000 FROM t_map WHERE if(m['k'] = 'v', 1, rand() % 100 = 0) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'rand, index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE if(m['k'] = 'v', 1, rand() % 100 = 0) SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: ki%';
SELECT 'control: length, rows', count() FROM t_map WHERE length(m['k']) > 0;
SELECT 'control: length, index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map WHERE length(m['k']) > 0 SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: ki%';

-- Every second granule has the path `a`, the others only the path `z`.
CREATE TABLE t_json (id UInt64, j JSON, INDEX ji JSONAllPaths(j) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, text_index_posting_list_block_size = 1048576;
INSERT INTO t_json SELECT number, if(intDiv(number, 8) % 2 = 0, '{"a": 1}', '{"z": 1}') FROM numbers(8000);

SELECT '-- text index on JSON paths';
SELECT 'json path', count() > 4000 FROM t_json WHERE if(j.a::Int64 = 1, 1, rand() % 100 = 0);
SELECT 'rand, index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json WHERE if(j.a::Int64 = 1, 1, rand() % 100 = 0) SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: ji%';
SELECT 'control: rows', count() FROM t_json WHERE j.a::Int64 = 1;
SELECT 'control: index used', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json WHERE j.a::Int64 = 1 SETTINGS enable_parallel_replicas = 0, use_query_condition_cache = 0) WHERE explain LIKE '%Name: ji%';

DROP TABLE t_set;
DROP TABLE t_map;
DROP TABLE t_json;
DROP TABLE t_name;
