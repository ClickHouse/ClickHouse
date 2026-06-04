SELECT '--- Test 1: Int32 column used as boolean condition in WHERE';

DROP TABLE IF EXISTS test_bool_index;

CREATE TABLE test_bool_index (id Int32, value String) 
ENGINE = MergeTree 
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_bool_index SELECT number, toString(number) FROM numbers(1000);

EXPLAIN indexes = 1, description = 0 SELECT * FROM test_bool_index WHERE id;

DROP TABLE test_bool_index;

SELECT '--- Test 2: Float column used as boolean condition in WHERE';

DROP TABLE IF EXISTS test_bool_float;

CREATE TABLE test_bool_float (x Float64, y String)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 8192;

INSERT INTO test_bool_float VALUES (0, 'zero'), (NULL, 'nan'), (1.5, 'positive'), (-2.5, 'negative');

EXPLAIN indexes = 1, description = 0 SELECT * FROM test_bool_float WHERE x;

SELECT x FROM test_bool_float WHERE x ORDER BY y;

DROP TABLE test_bool_float;

SELECT '--- Test 3: Nullable column used as boolean condition in WHERE';

DROP TABLE IF EXISTS test_bool_nullable;

CREATE TABLE test_bool_nullable (id Nullable(Int32), value String)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO test_bool_nullable VALUES (NULL, 'null'), (0, 'zero'), (1, 'one'), (2, 'two');

EXPLAIN indexes = 1, description = 0 SELECT * FROM test_bool_nullable WHERE id;

SELECT id, value FROM test_bool_nullable WHERE id ORDER BY value;

DROP TABLE test_bool_nullable;

SELECT '--- Test 4: Bool column used as boolean condition in WHERE';

DROP TABLE IF EXISTS test_bool_flag;

CREATE TABLE test_bool_flag (flag Bool, value String)
ENGINE = MergeTree
ORDER BY flag
SETTINGS index_granularity = 8192;

INSERT INTO test_bool_flag VALUES (false, 'no'), (true, 'yes');

EXPLAIN indexes = 1, description = 0 SELECT * FROM test_bool_flag WHERE flag;

SELECT value FROM test_bool_flag WHERE flag ORDER BY value;

DROP TABLE test_bool_flag;

SELECT '--- Test 5: WHERE NOT id (negated bare column)';

DROP TABLE IF EXISTS test_not_id;

CREATE TABLE test_not_id (id Int32, value String)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_not_id SELECT number, toString(number) FROM numbers(1000);

SELECT id, value FROM test_not_id WHERE NOT id SETTINGS force_primary_key = 1;

DROP TABLE test_not_id;

SELECT '--- Test 6: Minmax skip index with bare column';

DROP TABLE IF EXISTS test_minmax_skip;

CREATE TABLE test_minmax_skip (id Int32, value String, INDEX id_minmax id TYPE minmax GRANULARITY 1)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO test_minmax_skip VALUES (0, 'zero'), (1, 'one'), (2, 'two');

SELECT id, value FROM test_minmax_skip WHERE id SETTINGS force_data_skipping_indices = 'id_minmax';

DROP TABLE test_minmax_skip;
