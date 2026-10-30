-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY: BACKUP TABLE throws SUPPORT_IS_DISABLED.

SET enable_unique_key = 1;

-- Red if BACKUP stops refusing a UNIQUE KEY table.
DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

BACKUP TABLE uk_guard TO Null FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE uk_guard;
