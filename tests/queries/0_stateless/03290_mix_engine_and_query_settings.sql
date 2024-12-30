SET enable_json_type = 0;

DROP TABLE IF EXISTS example_mt;
CREATE TABLE example_mt
(
    `id` UInt32,
    `data` JSON
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS async_insert = 1, enable_json_type = 1;
SHOW CREATE TABLE example_mt;
DROP TABLE IF EXISTS example_mt;

DROP TABLE IF EXISTS example_memory;
CREATE TABLE example_memory
(
    `id` UInt64,
    `data` JSON
)
ENGINE = Memory
SETTINGS max_rows_to_keep = 100, enable_json_type = 1;
SHOW CREATE TABLE example_memory;
DROP TABLE IF EXISTS example_memory;


DROP TABLE IF EXISTS example_set;
CREATE TABLE example_set
(
    `id` UInt64,
    `data` JSON
)
ENGINE = Set
SETTINGS persistent = 1, enable_json_type = 1;
SHOW CREATE TABLE example_set;
DROP TABLE IF EXISTS example_set;

DROP TABLE IF EXISTS example_join;
CREATE TABLE example_join
(
    `id` UInt64,
    `data` JSON
)
ENGINE = Join(ANY, LEFT, id)
SETTINGS persistent = 1, enable_json_type = 1;
SHOW CREATE TABLE example_join;
DROP TABLE IF EXISTS example_join;
