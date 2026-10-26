-- Tags: no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- no-parallel: ATTACHes a table with a fixed UUID, which collides across
-- concurrent runs of this test (e.g. the flaky check's parallel workers).
-- TTL is enforced during merges, which are disabled on UNIQUE KEY tables (interim,
-- until merge-side bitmap reconciliation lands), so it cannot be honored. Every TTL
-- entry path is rejected with SUPPORT_IS_DISABLED: CREATE-with-TTL (table or column),
-- ALTER MODIFY TTL, per-column TTL via ALTER ADD/MODIFY COLUMN, and ALTER MATERIALIZE
-- TTL. REMOVE TTL stays allowed and ATTACH is exempt (loads a pre-existing UK+TTL
-- table); non-UNIQUE-KEY tables are unaffected. All keys distinct (dedup is a later PR).

SET allow_experimental_unique_key = 1;

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
ALTER TABLE uk_ttl MODIFY COLUMN w LowCardinality(String);

DROP TABLE uk_ttl;

-- ATTACH is exempt: a pre-existing UNIQUE KEY + TTL table still loads (the guard is
-- CREATE-only). Fixed UUID for the Atomic database engine (cf. 04046).
DROP TABLE IF EXISTS uk_ttl_attach SYNC;
ATTACH TABLE uk_ttl_attach UUID '00000000-0000-0000-0000-000000004159'
(id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
TTL d + INTERVAL 1 DAY;

-- MATERIALIZE TTL on the loaded UK+TTL table is rejected (it rewrites parts outside the
-- UNIQUE KEY dedup path, and TTL is unsupported while merges are disabled).
ALTER TABLE uk_ttl_attach MATERIALIZE TTL; -- { serverError SUPPORT_IS_DISABLED }

-- REMOVE TTL is allowed — a distinct AlterCommand type that only clears the expression.
ALTER TABLE uk_ttl_attach REMOVE TTL;

DROP TABLE uk_ttl_attach SYNC;

-- Non-UNIQUE-KEY table with TTL is unaffected by the guard.
DROP TABLE IF EXISTS plain_ttl;
CREATE TABLE plain_ttl (id UInt64, d Date) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY;
DROP TABLE plain_ttl;

SELECT 'ok' AS step;
