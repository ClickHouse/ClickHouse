-- Verify that DROP COLUMN + ADD COLUMN with the same name does not resurrect old data.
-- When a DROP COLUMN mutation has not yet been applied to a part,
-- re-adding the column must return defaults, not stale data from before the drop.

DROP TABLE IF EXISTS t_drop_readd;

CREATE TABLE t_drop_readd (key UInt64, value String DEFAULT 'new')
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_drop_readd (key, value) VALUES (1, 'old_data');

-- Stop merges to prevent the DROP COLUMN mutation from being applied.
SYSTEM STOP MERGES t_drop_readd;

-- alter_sync = 0 so that ALTER DROP COLUMN does not wait for the mutation to finish
-- (it cannot finish because merges are stopped).
SET alter_sync = 0;

ALTER TABLE t_drop_readd DROP COLUMN value;
ALTER TABLE t_drop_readd ADD COLUMN value String DEFAULT 'new';

-- The DROP mutation has not been applied yet (merges are stopped).
-- Reading must return the default 'new', not the stale 'old_data'.
SELECT key, value FROM t_drop_readd ORDER BY key;

-- Now let the mutations run and verify the result is the same.
SYSTEM START MERGES t_drop_readd;
SET alter_sync = 2;
-- Trigger a no-op mutation to wait for all pending mutations to finish.
ALTER TABLE t_drop_readd MODIFY SETTING parts_to_delay_insert = 0;

SELECT key, value FROM t_drop_readd ORDER BY key;

DROP TABLE t_drop_readd;
