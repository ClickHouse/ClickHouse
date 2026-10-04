-- The second and third joins get their arrays lazily replicated from the previous one.
SET enable_lazy_columns_replication = 1;

DROP TABLE IF EXISTS t_lazy_arrays;
CREATE TABLE t_lazy_arrays (id UInt32, a Array(String), b Array(UInt32), c Array(String), m Map(String, UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lazy_arrays SELECT number, arrayMap(i -> concat('a', toString(number), '_', toString(i)), range(number % 3)), range(number % 4), arrayMap(i -> concat('c', toString(i)), range(number % 4)), map('k', number, 'kk', number * 2) FROM numbers(7);

-- Results must not depend on lazy replication.
SELECT id, x, y, kv FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y ARRAY JOIN m AS kv ORDER BY ALL SETTINGS max_block_size = 3;
SELECT count(), sum(cityHash64(id, x, y, kv)) FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y ARRAY JOIN m AS kv SETTINGS max_block_size = 3;
SELECT count(), sum(cityHash64(id, x, y, kv)) FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y ARRAY JOIN m AS kv SETTINGS max_block_size = 3, enable_lazy_columns_replication = 0;
SELECT count(), sum(cityHash64(id, x, y, kv)) FROM t_lazy_arrays LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN m AS kv SETTINGS max_block_size = 3;
SELECT count(), sum(cityHash64(id, x, y, kv)) FROM t_lazy_arrays LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN m AS kv SETTINGS max_block_size = 3, enable_lazy_columns_replication = 0;
SELECT count(), sum(cityHash64(id, x, y, z)) FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y, c AS z SETTINGS max_block_size = 3;
SELECT count(), sum(cityHash64(id, x, y, z)) FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y, c AS z SETTINGS max_block_size = 3, enable_lazy_columns_replication = 0;
SELECT count(), sum(cityHash64(id, x, y)) FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS max_block_size = 3;
SELECT count(), sum(cityHash64(id, x, y)) FROM t_lazy_arrays ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS max_block_size = 3, enable_lazy_columns_replication = 0;

DROP TABLE t_lazy_arrays;

DROP TABLE IF EXISTS t_lazy_arrays_wide;
CREATE TABLE t_lazy_arrays_wide (id UInt32, a Array(String), b Array(String), c Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lazy_arrays_wide SELECT number, arrayMap(i -> concat('a', toString(number), '_', toString(i), 'xxxxxxxxxx'), range(100)), arrayMap(i -> concat('b', toString(number), '_', toString(i), 'xxxxxxxxxx'), range(100)), arrayMap(i -> concat('c', toString(number), '_', toString(i), 'xxxxxxxxxx'), range(100)) FROM numbers(1000);

-- Used to materialize the whole block of arrays in every join, way above this limit.
SELECT count() FROM (SELECT x, y, z FROM t_lazy_arrays_wide ARRAY JOIN a AS x ARRAY JOIN b AS y ARRAY JOIN c AS z LIMIT 10) SETTINGS max_block_size = 65409, max_threads = 1, enable_lazy_columns_replication = 1, max_memory_usage = 100000000;
SELECT count() FROM (SELECT x, y, z FROM t_lazy_arrays_wide LEFT ARRAY JOIN a AS x LEFT ARRAY JOIN b AS y LEFT ARRAY JOIN c AS z LIMIT 10) SETTINGS max_block_size = 65409, max_threads = 1, enable_lazy_columns_replication = 1, max_memory_usage = 100000000;
SELECT count() FROM (SELECT arrayJoin(a) AS x, arrayJoin(b) AS y, arrayJoin(c) AS z FROM t_lazy_arrays_wide WHERE id >= 0 LIMIT 10) SETTINGS max_block_size = 65409, max_threads = 1, enable_lazy_columns_replication = 1, max_memory_usage = 100000000, query_plan_lower_array_join_function = 1;

DROP TABLE t_lazy_arrays_wide;
