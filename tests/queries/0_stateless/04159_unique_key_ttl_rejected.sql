-- Tags: no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- no-parallel: ATTACHes a table with a fixed UUID, which collides across
-- concurrent runs of this test (e.g. the flaky check's parallel workers).
-- TTL is enforced during merges, which are disabled on UNIQUE KEY tables (interim,
-- until merge-side bitmap reconciliation lands), so it cannot be honored. Every TTL
-- entry path is rejected with SUPPORT_IS_DISABLED: CREATE-with-TTL (table or column),
-- ALTER MODIFY TTL, per-column TTL via ALTER ADD/MODIFY COLUMN, and ALTER MATERIALIZE
-- TTL, also by a full-definition ATTACH. Non-UNIQUE-KEY tables are unaffected. All keys
-- distinct (dedup is a later PR).

SET enable_unique_key = 1;

DROP TABLE IF EXISTS uk_ttl;

-- Table TTL rejected at CREATE.
CREATE TABLE uk_ttl (id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

-- Column TTL rejected at CREATE.
CREATE TABLE uk_ttl (id UInt64, d Date, v String TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id); -- { serverError SUPPORT_IS_DISABLED }

-- Without TTL: creates fine.
CREATE TABLE uk_ttl (id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

-- ALTER MODIFY TTL rejected on the existing UNIQUE KEY table.
ALTER TABLE uk_ttl MODIFY TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

-- Per-column TTL via ALTER ADD/MODIFY COLUMN rejected (the non-MODIFY_TTL surface).
ALTER TABLE uk_ttl ADD COLUMN w String TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_ttl MODIFY COLUMN v String TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

-- A column ALTER with no TTL clause is unaffected (command.ttl is null).
ALTER TABLE uk_ttl ADD COLUMN w String;
ALTER TABLE uk_ttl MODIFY COLUMN w String DEFAULT 'w';

DROP TABLE uk_ttl;

-- A full-definition ATTACH is rejected like CREATE. A stored UNIQUE KEY + TTL table still loads,
-- see `unique_key_ttl_stored_definition`. Fixed UUID for the Atomic database engine (cf. 04046).
ATTACH TABLE uk_ttl_attach UUID '00000000-0000-0000-0000-000000004159'
(id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

-- Non-UNIQUE-KEY table with TTL is unaffected by the guard.
DROP TABLE IF EXISTS plain_ttl;
CREATE TABLE plain_ttl (id UInt64, d Date) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY;
DROP TABLE plain_ttl;

SELECT 'ok' AS step;
