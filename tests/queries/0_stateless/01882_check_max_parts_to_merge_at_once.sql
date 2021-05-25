DROP TABLE IF EXISTS limited_merge_table;

SET max_threads = 1;
SET max_block_size = 1;
SET min_insert_block_size_rows = 1;

CREATE TABLE limited_merge_table
(
    key UInt64
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS max_parts_to_merge_at_once = 3;

SYSTEM STOP MERGES limited_merge_table;

INSERT INTO limited_merge_table SELECT number FROM numbers(250);


SYSTEM START MERGES limited_merge_table;

OPTIMIZE TABLE limited_merge_table FINAL;

SYSTEM FLUSH LOGS;

-- final optimize FINAL will merge all parts, but all previous merges must merge <= 3 parts
SELECT length(merged_from) <= 3 FROM system.part_log WHERE event_type = 'MergeParts' and table = 'limited_merge_table' and database = currentDatabase() ORDER BY length(merged_from) DESC LIMIT 1 OFFSET 2;

DROP TABLE IF EXISTS limited_merge_table;
