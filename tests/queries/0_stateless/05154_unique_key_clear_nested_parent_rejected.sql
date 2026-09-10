-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- Regression: `CLEAR COLUMN n` on a UNIQUE KEY table, where `n` is a flattened
-- Nested group, must be rejected by the whole-part-rewrite guard. The guard used
-- the exact-only `ColumnsDescription::hasPhysical(n)`, which is false for a
-- Nested parent stored as its members (`n.x` / `n.y`), so the command bypassed
-- the guard, reached the part-rewrite path, and dropped the per-part
-- `unique_key_index.sst`. The guard now uses the nested-aware
-- `hasColumnOrNested(GetColumnsOptions::AllPhysical, ...)`.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_clear_nested;
CREATE TABLE uk_clear_nested (id UInt32, n Nested(x UInt32, y UInt32))
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_nested VALUES (1, [10], [20]), (2, [11], [21]);

-- The Nested parent is a stored target: the mutation path expands it to its
-- members and rewrites the whole part, so the guard must reject it.
SELECT 'clear_nested_parent_rejected' AS step;
ALTER TABLE uk_clear_nested CLEAR COLUMN n IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

-- A nested member is a stored target as well, same rejection (unchanged behavior).
SELECT 'clear_nested_member_rejected' AS step;
ALTER TABLE uk_clear_nested CLEAR COLUMN n.x IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

-- A genuinely missing name with IF EXISTS rewrites nothing, still a silent no-op.
SELECT 'clear_missing_if_exists_noop' AS step;
ALTER TABLE uk_clear_nested CLEAR COLUMN IF EXISTS nope IN PARTITION ID 'all';

-- The rejected commands leave data and the UNIQUE KEY index intact.
SELECT 'state_intact', count(), groupArray(n.x), groupArray(n.y) FROM uk_clear_nested;

DROP TABLE uk_clear_nested;
