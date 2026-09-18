-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- Regression: `hasColumnOrNested` used to inspect only the first `n.*` match, so a UNIQUE KEY
-- table with `` `n.x` UInt32 ALIAS id, `n.y` UInt32 `` treated `CLEAR COLUMN n` as a no-op.
-- The physical `n.y` must make the parent a stored target and the UNIQUE KEY guard must reject.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

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
