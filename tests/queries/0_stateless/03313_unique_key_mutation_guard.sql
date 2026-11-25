-- Tags: no-ordinary-database, no-async-insert, no-fasttest
-- UNIQUE KEY: which mutations run on the table and which are rejected.
--   1. ALTER DELETE / UPDATE: rejected, heavy and lightweight
--   2. MATERIALIZE / CLEAR COLUMN: rejected for any column, a Nested group included; CLEAR IF EXISTS of a missing one is a no-op
--   2a. part rewrites: REWRITE PARTS, APPLY DELETED MASK / PATCHES, MATERIALIZE INDEX / STATISTICS / PROJECTION are rejected
--   4. plain table: the same operations still work without UNIQUE KEY

SET enable_unique_key = 1;

DROP TABLE IF EXISTS uk_mut_guard;
CREATE TABLE uk_mut_guard (a UInt32, b UInt32, c UInt32, d String DEFAULT 'def')
ENGINE = MergeTree ORDER BY (c) UNIQUE KEY (a, b);

INSERT INTO uk_mut_guard VALUES (1, 10, 100, 'x'), (2, 20, 200, 'y');

-- 1. ALTER DELETE / UPDATE: red if the mutation guard admits them, or a lightweight UPDATE stops
-- falling back to the heavy mutation on a UNIQUE KEY table.
SELECT 'alter_delete_uk' AS step;
ALTER TABLE uk_mut_guard DELETE WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter_update_uk_column' AS step;
ALTER TABLE uk_mut_guard UPDATE a = 99 WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter_update_non_uk_column' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'lightweight_update_uk' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1
SETTINGS alter_update_mode = 'lightweight', enable_lightweight_update = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'lightweight_force_update_uk' AS step;
ALTER TABLE uk_mut_guard UPDATE d = 'z' WHERE a = 1
SETTINGS alter_update_mode = 'lightweight_force', enable_lightweight_update = 1; -- { serverError SUPPORT_IS_DISABLED }

-- 2. MATERIALIZE / CLEAR COLUMN: red if the mutation guard admits MATERIALIZE or CLEAR COLUMN, or
-- stops checking the mutations an ALTER derives (CLEAR COLUMN reaches no other check).
SELECT 'materialize_uk_a' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN a; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'materialize_uk_b' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN b; -- { serverError SUPPORT_IS_DISABLED }

-- A key column hits the key-column guard first (ALTER_OF_COLUMN_IS_FORBIDDEN).
SELECT 'clear_uk_a' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN a IN PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT 'clear_uk_b' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN b IN PARTITION ID 'all'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SET mutations_sync = 2;
SELECT 'materialize_non_uk_d' AS step;
ALTER TABLE uk_mut_guard MATERIALIZE COLUMN d; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'clear_non_uk_d' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN d IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'clear_missing_if_exists_noop' AS step;
ALTER TABLE uk_mut_guard CLEAR COLUMN IF EXISTS nope IN PARTITION ID 'all';

-- 2a. part rewrites: red if the mutation guard admits one. It fires
-- before name resolution, so the names need not exist.
ALTER TABLE uk_mut_guard REWRITE PARTS; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard APPLY DELETED MASK; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard APPLY PATCHES; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard MATERIALIZE INDEX idx; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard MATERIALIZE STATISTICS v; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_mut_guard MATERIALIZE PROJECTION proj; -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM uk_mut_guard;  -- 2

DROP TABLE uk_mut_guard;

-- CLEAR of a Nested group names no physical column, only its `n.x` / `n.y` arrays.
DROP TABLE IF EXISTS uk_mut_nested;
CREATE TABLE uk_mut_nested (id UInt32, n Nested(x UInt32, y String))
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS share_nested_offsets = 1;

INSERT INTO uk_mut_nested VALUES (1, [1, 2], ['a', 'b']), (2, [3], ['c']);

SELECT 'clear_nested_group' AS step;
ALTER TABLE uk_mut_nested CLEAR COLUMN n IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT id, n.x, n.y FROM uk_mut_nested ORDER BY id;

DROP TABLE uk_mut_nested;

-- 4. plain table: red if the mutation guard, on a mutation or on an ALTER, applies to a table
-- without UNIQUE KEY.
DROP TABLE IF EXISTS mt_plain;
CREATE TABLE mt_plain (a UInt32, b UInt32 DEFAULT 0, d String DEFAULT 'def')
ENGINE = MergeTree ORDER BY a;

INSERT INTO mt_plain VALUES (1, 10, 'x'), (2, 20, 'y');

SET mutations_sync = 2;
ALTER TABLE mt_plain MATERIALIZE COLUMN b;
ALTER TABLE mt_plain CLEAR COLUMN b IN PARTITION ID 'all';
ALTER TABLE mt_plain MATERIALIZE COLUMN d;
ALTER TABLE mt_plain CLEAR COLUMN d IN PARTITION ID 'all';

SELECT count() FROM mt_plain;  -- 2

DROP TABLE mt_plain;
