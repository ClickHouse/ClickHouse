-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY interim guards: BACKUP and CHECK TABLE are rejected on tables with
-- a UNIQUE KEY. Both operations would mishandle the per-part delete-bitmap
-- sidecars (BACKUP omits them; CHECK flags them as unexpected files). Real
-- sidecar-aware support is deferred to a follow-up PR. All keys distinct.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

INSERT INTO uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c');

BACKUP TABLE uk_guard TO Null FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }
CHECK TABLE uk_guard; -- { serverError SUPPORT_IS_DISABLED }

-- A plain MergeTree table (no UNIQUE KEY) is unaffected: CHECK still works.
DROP TABLE IF EXISTS plain_guard;
CREATE TABLE plain_guard (id UInt64, v String) ENGINE = MergeTree ORDER BY (id);
INSERT INTO plain_guard VALUES (1, 'a'), (2, 'b');
CHECK TABLE plain_guard FORMAT Null;

DROP TABLE uk_guard;
DROP TABLE plain_guard;
