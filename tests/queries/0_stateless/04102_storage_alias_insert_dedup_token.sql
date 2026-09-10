SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS dedup_alias_target;
DROP TABLE IF EXISTS dedup_alias_table;

CREATE TABLE dedup_alias_target (id UInt32)
ENGINE = MergeTree
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE dedup_alias_table ENGINE = Alias('dedup_alias_target');

SET max_block_size = 2;
SET max_insert_block_size = 2;
SET max_insert_threads = 3;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

SET insert_deduplication_token = 'alias_dedup_test';
INSERT INTO dedup_alias_table SELECT number FROM numbers(10);
SELECT 'alias_with_token', count() FROM dedup_alias_target;

TRUNCATE TABLE dedup_alias_target;

SET insert_deduplication_token = 'direct_dedup_test';
INSERT INTO dedup_alias_target SELECT number FROM numbers(10);
SELECT 'direct_with_token', count() FROM dedup_alias_target;

TRUNCATE TABLE dedup_alias_target;

SET insert_deduplication_token = '';
INSERT INTO dedup_alias_table SELECT number FROM numbers(10);
SELECT 'alias_without_token', count() FROM dedup_alias_target;

DROP TABLE dedup_alias_table;
DROP TABLE dedup_alias_target;
