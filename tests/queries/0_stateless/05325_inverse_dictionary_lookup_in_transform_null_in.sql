-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- With `transform_null_in = 1`, `IN` / `NOT IN` are resolved as `nullIn` / `notNullIn`.
-- `optimize_inverse_dictionary_lookup` should still rewrite them.

SET enable_analyzer = 1;
SET rewrite_in_to_join = 0;
SET transform_null_in = 1;
SET optimize_inverse_dictionary_lookup = 1;

DROP DICTIONARY IF EXISTS colors_05325;
DROP TABLE IF EXISTS ref_colors_05325;
DROP TABLE IF EXISTS t_05325;

CREATE TABLE ref_colors_05325 (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ref_colors_05325 VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green');

CREATE DICTIONARY colors_05325 (id UInt64, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ref_colors_05325'))
LAYOUT(HASHED())
LIFETIME(0);

CREATE TABLE t_05325 (color_id UInt64) ENGINE = MergeTree ORDER BY color_id;
INSERT INTO t_05325 VALUES (1), (2), (3), (4), (99);

SELECT 'IN plan: dictGet calls left';
SELECT countIf(explain LIKE '%dictGetString%') FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) IN ('red', 'blue')
);

SELECT 'NOT IN plan: dictGet calls left';
SELECT countIf(explain LIKE '%dictGetString%') FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) NOT IN ('red', '')
);

SELECT 'IN results';
SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) IN ('red', 'blue') ORDER BY color_id;
SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) IN ('red', 'blue') ORDER BY color_id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'IN with default results';
SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) IN ('red', '') ORDER BY color_id;
SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) IN ('red', '') ORDER BY color_id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'NOT IN results';
SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) NOT IN ('red', '') ORDER BY color_id;
SELECT color_id FROM t_05325 WHERE dictGetString('colors_05325', 'name', color_id) NOT IN ('red', '') ORDER BY color_id SETTINGS optimize_inverse_dictionary_lookup = 0;

DROP DICTIONARY colors_05325;
DROP TABLE ref_colors_05325;
DROP TABLE t_05325;
