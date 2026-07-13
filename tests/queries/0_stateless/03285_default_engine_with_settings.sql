-- Tags: memory-engine
-- https://github.com/ClickHouse/ClickHouse/issues/73099

DROP TABLE IF EXISTS example_table;
DROP TABLE IF EXISTS example_table2;

set default_table_engine = 'MergeTree';
CREATE TABLE example_table (id UInt32, data String) ORDER BY id SETTINGS max_part_loading_threads=8;
SHOW CREATE TABLE example_table;

SET default_table_engine = 'Memory';
-- Memory with ORDER BY is wrong
CREATE TABLE example_table2 (id UInt32, data String) ORDER BY id SETTINGS max_part_loading_threads=8; -- { serverError BAD_ARGUMENTS }

-- Memory with max_part_loading_threads is wrong
CREATE TABLE example_table2 (id UInt32, data String) SETTINGS max_part_loading_threads=8; -- { serverError UNKNOWN_SETTING }

CREATE TABLE example_table2 (id UInt32, data String) SETTINGS max_rows_to_keep=42;
SHOW CREATE TABLE example_table2;

DROP TABLE IF EXISTS example_table;
DROP TABLE IF EXISTS example_table2;
