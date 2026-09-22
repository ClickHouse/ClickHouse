-- Settings metadata updates must preserve nonempty lists and unrelated storage clauses.
DROP TABLE IF EXISTS settings_reset_mt;
DROP TABLE IF EXISTS settings_reset_memory;

CREATE TABLE settings_reset_mt (id UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS max_parts_in_total = 12345, min_bytes_for_wide_part = 4096;
INSERT INTO settings_reset_mt VALUES (1), (2);

ALTER TABLE settings_reset_mt RESET SETTING max_parts_in_total;
SELECT 'partial_reset',
    position(create_table_query, 'max_parts_in_total') = 0,
    position(create_table_query, 'min_bytes_for_wide_part = 4096') > 0,
    sorting_key
FROM system.tables
WHERE database = currentDatabase() AND name = 'settings_reset_mt';

ALTER TABLE settings_reset_mt RESET SETTING min_bytes_for_wide_part;
ALTER TABLE settings_reset_mt RESET SETTING min_bytes_for_wide_part;
DETACH TABLE settings_reset_mt SYNC;
ATTACH TABLE settings_reset_mt;
SELECT 'reattach',
    position(create_table_query, 'max_parts_in_total') = 0,
    position(create_table_query, 'min_bytes_for_wide_part') = 0,
    sorting_key
FROM system.tables
WHERE database = currentDatabase() AND name = 'settings_reset_mt';
SELECT count(), sum(id) FROM settings_reset_mt;

ALTER TABLE settings_reset_mt MODIFY SETTING max_parts_in_total = 12345;
SELECT 'modify_again', position(create_table_query, 'max_parts_in_total = 12345') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 'settings_reset_mt';

-- Exercise both adding settings to a simple storage definition and retaining them on a comment change.
CREATE TABLE settings_reset_memory (id UInt64) ENGINE = Memory;
ALTER TABLE settings_reset_memory COMMENT COLUMN id 'before settings';
ALTER TABLE settings_reset_memory MODIFY SETTING compress = 1;
ALTER TABLE settings_reset_memory COMMENT COLUMN id 'after settings';
SELECT 'memory_settings', position(create_table_query, 'compress = 1') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 'settings_reset_memory';
INSERT INTO settings_reset_memory VALUES (1), (2);
SELECT count(), sum(id) FROM settings_reset_memory;

DROP TABLE settings_reset_mt;
DROP TABLE settings_reset_memory;
