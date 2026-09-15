-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- Regression: `CLEAR COLUMN n` on a UNIQUE KEY table rewrites the whole part and loses the
-- per-part `unique_key_index.sst`, so the guard must reject it when `n` names a stored column --
-- including a flattened Nested parent (stored as `n.x` / `n.y`). With `share_nested_offsets = 0`
-- a dotted scalar is independent, so `CLEAR COLUMN IF EXISTS n` is a no-op and must pass.

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

-- With `share_nested_offsets = 0` the dotted scalar `n.x` is an independent column, so `n` is
-- not a stored target and the conditional clear is a no-op.
DROP TABLE IF EXISTS uk_clear_no_share;
CREATE TABLE uk_clear_no_share (id UInt32, `n.x` UInt32)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS share_nested_offsets = 0;

INSERT INTO uk_clear_no_share VALUES (1, 10);

SELECT 'clear_parent_no_share_noop' AS step;
ALTER TABLE uk_clear_no_share CLEAR COLUMN IF EXISTS n IN PARTITION ID 'all';
SELECT 'state_intact_no_share', * FROM uk_clear_no_share;

DROP TABLE uk_clear_no_share;
