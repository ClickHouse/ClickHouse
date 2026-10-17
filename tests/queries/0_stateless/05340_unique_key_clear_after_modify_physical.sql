-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- Same-statement `MODIFY COLUMN` that makes a column physical must be visible to a later
-- `CLEAR COLUMN` on a UNIQUE KEY table, or the rewrite would drop `unique_key_index.sst`.

SET enable_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_clear_after_alias;
CREATE TABLE uk_clear_after_alias (id UInt32, x UInt32 ALIAS id)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_after_alias (id) VALUES (1), (2);

SELECT 'clear_after_alias_to_default_rejected' AS step;
ALTER TABLE uk_clear_after_alias
    MODIFY COLUMN x UInt32 DEFAULT 0,
    CLEAR COLUMN x IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'state_intact_alias', id, x FROM uk_clear_after_alias ORDER BY id;
SELECT 'kind_intact_alias', name, default_kind FROM system.columns
    WHERE database = currentDatabase() AND table = 'uk_clear_after_alias' AND name = 'x';

DROP TABLE uk_clear_after_alias;

DROP TABLE IF EXISTS uk_clear_after_remove_alias;
CREATE TABLE uk_clear_after_remove_alias (id UInt32, x UInt32 ALIAS id)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_after_remove_alias (id) VALUES (1), (2);

SELECT 'clear_after_remove_alias_rejected' AS step;
ALTER TABLE uk_clear_after_remove_alias
    MODIFY COLUMN x REMOVE ALIAS,
    CLEAR COLUMN x IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'state_intact_remove_alias', id, x FROM uk_clear_after_remove_alias ORDER BY id;
SELECT 'kind_intact_remove_alias', name, default_kind FROM system.columns
    WHERE database = currentDatabase() AND table = 'uk_clear_after_remove_alias' AND name = 'x';

DROP TABLE uk_clear_after_remove_alias;

DROP TABLE IF EXISTS uk_clear_after_ephemeral;
CREATE TABLE uk_clear_after_ephemeral (id UInt32, x UInt32 EPHEMERAL 0)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO uk_clear_after_ephemeral (id) VALUES (1), (2);

SELECT 'clear_after_ephemeral_to_default_rejected' AS step;
ALTER TABLE uk_clear_after_ephemeral
    MODIFY COLUMN x UInt32 DEFAULT 0,
    CLEAR COLUMN x IN PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'state_intact_ephemeral', id FROM uk_clear_after_ephemeral ORDER BY id;
SELECT 'kind_intact_ephemeral', name, default_kind FROM system.columns
    WHERE database = currentDatabase() AND table = 'uk_clear_after_ephemeral' AND name = 'x';

DROP TABLE uk_clear_after_ephemeral;
