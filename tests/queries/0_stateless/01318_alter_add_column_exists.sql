DROP TABLE IF EXISTS add_table;

CREATE TABLE add_table
(
    key UInt64,
    value1 String
)
ENGINE = MergeTree()
ORDER BY key;

SHOW CREATE TABLE add_table;

ALTER TABLE add_table ADD COLUMN IF NOT EXISTS value1 UInt64;

SHOW CREATE TABLE add_table;

DROP TABLE IF EXISTS add_table;
