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

DROP TABLE IF EXISTS add_nested;

-- With flatten_nested (default on) `n Nested(a ...)` is stored as `n.a`, so a second
-- IF NOT EXISTS add of the same nested group must be a no-op at apply time as well
-- (has("n") is false while the flattened `n.a` add would otherwise collide).
CREATE TABLE add_nested
(
    key UInt64
)
ENGINE = MergeTree()
ORDER BY key;

ALTER TABLE add_nested ADD COLUMN IF NOT EXISTS n Nested(a UInt32), ADD COLUMN IF NOT EXISTS n Nested(a UInt32);

SHOW CREATE TABLE add_nested;

-- Multi-field nested group added twice with IF NOT EXISTS: added once, second is a no-op.
ALTER TABLE add_nested ADD COLUMN IF NOT EXISTS m Nested(a UInt32, b String), ADD COLUMN IF NOT EXISTS m Nested(a UInt32, b String);

SHOW CREATE TABLE add_nested;

DROP TABLE IF EXISTS add_nested;

DROP TABLE IF EXISTS add_nested_no_share;

-- Same with share_nested_offsets disabled: the flattened `p.a` add still collides, so IF NOT EXISTS is a no-op.
CREATE TABLE add_nested_no_share
(
    key UInt64
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS share_nested_offsets = 0;

ALTER TABLE add_nested_no_share ADD COLUMN IF NOT EXISTS p Nested(a UInt32), ADD COLUMN IF NOT EXISTS p Nested(a UInt32);

SHOW CREATE TABLE add_nested_no_share;

DROP TABLE IF EXISTS add_nested_no_share;

DROP TABLE IF EXISTS add_nested_prefix;

-- The apply-time guard must compare exact transformed names, not an `n.*` prefix: a table that
-- already has only `n.a` must NOT skip a genuinely new, distinct top-level scalar `n`. Otherwise
-- IF NOT EXISTS would silently drop a valid ADD COLUMN.
CREATE TABLE add_nested_prefix
(
    key UInt64,
    `n.a` UInt32
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS share_nested_offsets = 0;

ALTER TABLE add_nested_prefix ADD COLUMN IF NOT EXISTS n String;

SHOW CREATE TABLE add_nested_prefix;

DROP TABLE IF EXISTS add_nested_prefix;

DROP TABLE IF EXISTS add_nested_sno1;

-- With share_nested_offsets enabled (default), prepare()/validate() treat any pre-existing `n.*`
-- as "the whole nested column already exists": `ADD COLUMN n.a, ADD COLUMN IF NOT EXISTS n Nested(a, b)`
-- in one statement no-ops the second command (via hasNested("n")). apply() must match that contract,
-- so only `n.a` is added and `n.b` is NOT inserted (otherwise apply() would diverge from validate()).
CREATE TABLE add_nested_sno1
(
    key UInt64
)
ENGINE = MergeTree()
ORDER BY key;

ALTER TABLE add_nested_sno1 ADD COLUMN `n.a` Array(UInt32), ADD COLUMN IF NOT EXISTS n Nested(a UInt32, b String);

SHOW CREATE TABLE add_nested_sno1;

DROP TABLE IF EXISTS add_nested_sno1;

DROP TABLE IF EXISTS add_nested_sno0;

-- With share_nested_offsets disabled, `n` and `n.*` are independent, so the exact-name compare applies:
-- `ADD COLUMN n.a, ADD COLUMN IF NOT EXISTS n Nested(a, b)` skips only the colliding `n.a` and still
-- adds the genuinely new `n.b`.
CREATE TABLE add_nested_sno0
(
    key UInt64
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS share_nested_offsets = 0;

ALTER TABLE add_nested_sno0 ADD COLUMN `n.a` Array(UInt32), ADD COLUMN IF NOT EXISTS n Nested(a UInt32, b String);

SHOW CREATE TABLE add_nested_sno0;

DROP TABLE IF EXISTS add_nested_sno0;
