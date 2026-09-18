-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- UNIQUE KEY `CLEAR COLUMN n` is rejected for a stored Nested parent, including mixed
-- `ALIAS` + physical `n.*`. Missing/`ALIAS`-only prefixes with `IF EXISTS` stay no-ops.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_clear_nested;
CREATE TABLE uk_clear_nested (id UInt32, n Nested(x UInt32, y UInt32))
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_nested VALUES (1, [10], [20]), (2, [11], [21]);

SELECT 'clear_nested_parent_rejected' AS step;
ALTER TABLE uk_clear_nested CLEAR COLUMN n IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'clear_nested_member_rejected' AS step;
ALTER TABLE uk_clear_nested CLEAR COLUMN n.x IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'clear_missing_if_exists_noop' AS step;
ALTER TABLE uk_clear_nested CLEAR COLUMN IF EXISTS nope IN PARTITION ID 'all';

-- The rejected commands leave data and the UNIQUE KEY index intact.
SELECT 'state_intact', count(), groupArray(n.x), groupArray(n.y) FROM uk_clear_nested;

DROP TABLE uk_clear_nested;

-- `share_nested_offsets = 0`: dotted `n.x` is independent, so `CLEAR COLUMN IF EXISTS n` is a no-op.
DROP TABLE IF EXISTS uk_clear_no_share;
CREATE TABLE uk_clear_no_share (id UInt32, `n.x` UInt32)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS share_nested_offsets = 0;

INSERT INTO uk_clear_no_share VALUES (1, 10);

SELECT 'clear_parent_no_share_noop' AS step;
ALTER TABLE uk_clear_no_share CLEAR COLUMN IF EXISTS n IN PARTITION ID 'all';
SELECT 'state_intact_no_share', * FROM uk_clear_no_share;

DROP TABLE uk_clear_no_share;

-- Dotted `ALIAS` is not stored, so `CLEAR COLUMN IF EXISTS n` is a no-op.
DROP TABLE IF EXISTS uk_clear_dotted_alias;
CREATE TABLE uk_clear_dotted_alias (id UInt32, `n.x` UInt32 ALIAS id)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_dotted_alias (id) VALUES (1);

SELECT 'clear_parent_dotted_alias_noop' AS step;
ALTER TABLE uk_clear_dotted_alias CLEAR COLUMN IF EXISTS n IN PARTITION ID 'all';
SELECT 'state_intact_alias', id, `n.x` FROM uk_clear_dotted_alias;

DROP TABLE uk_clear_dotted_alias;

-- Mixed `ALIAS` then physical `n.*` is a stored target.
DROP TABLE IF EXISTS uk_clear_mixed_alias;
CREATE TABLE uk_clear_mixed_alias (id UInt32, `n.x` UInt32 ALIAS id, `n.y` UInt32)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_mixed_alias (id, `n.y`) VALUES (1, 10), (2, 20);

SELECT 'clear_mixed_alias_physical_rejected' AS step;
ALTER TABLE uk_clear_mixed_alias CLEAR COLUMN n IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'clear_mixed_alias_physical_if_exists_rejected' AS step;
ALTER TABLE uk_clear_mixed_alias CLEAR COLUMN IF EXISTS n IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'state_intact_mixed', id, `n.x`, `n.y` FROM uk_clear_mixed_alias ORDER BY id;

DROP TABLE uk_clear_mixed_alias;
