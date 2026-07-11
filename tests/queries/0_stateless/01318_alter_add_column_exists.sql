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

ALTER TABLE add_table ADD COLUMN IF NOT EXISTS key String, ADD COLUMN IF NOT EXISTS value1 UInt64;

SHOW CREATE TABLE add_table;

ALTER TABLE add_table ADD COLUMN IF NOT EXISTS value1 UInt64, ADD COLUMN IF NOT EXISTS value2 UInt64;

SHOW CREATE TABLE add_table;

-- ADD COLUMN adds value3, then ADD COLUMN IF NOT EXISTS of the same column is a no-op.
ALTER TABLE add_table ADD COLUMN value3 UInt64, ADD COLUMN IF NOT EXISTS value3 UInt32;

SHOW CREATE TABLE add_table;

-- Two IF NOT EXISTS of the same not-yet-existing column: added once, second is a no-op.
ALTER TABLE add_table ADD COLUMN IF NOT EXISTS value4 UInt64, ADD COLUMN IF NOT EXISTS value4 String;

SHOW CREATE TABLE add_table;

-- Plain ADD COLUMN of an already-existing column still throws.
ALTER TABLE add_table ADD COLUMN value1 UInt64; --{serverError DUPLICATE_COLUMN}

-- Two plain ADD COLUMN of the same new column in one statement still throws.
ALTER TABLE add_table ADD COLUMN value5 UInt64, ADD COLUMN value5 String; --{serverError DUPLICATE_COLUMN}

DROP TABLE IF EXISTS add_table;
