-- Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY + TTL is rejected on every entry path; an ATTACHed UK+TTL table cannot merge until REMOVE TTL.
--   1. CREATE and ALTER: table TTL, column TTL, MODIFY TTL, ADD/MODIFY COLUMN ... TTL all throw
--   2. ATTACH: loads; MATERIALIZE TTL, OPTIMIZE and DRY RUN throw; after REMOVE TTL it merges
--   3. plain table: TTL on a table without UNIQUE KEY is unaffected
-- no-parallel: case 2 ATTACHes a fixed UUID, which collides across concurrent runs.

SET enable_unique_key = 1;

-- 1. CREATE and ALTER: red if a CREATE or an ALTER stops refusing TTL.
DROP TABLE IF EXISTS uk_ttl;

CREATE TABLE uk_ttl (id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE uk_ttl (id UInt64, d Date, v String TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id); -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE uk_ttl (id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

ALTER TABLE uk_ttl MODIFY TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE uk_ttl ADD COLUMN w String TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_ttl MODIFY COLUMN v String TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE uk_ttl ADD COLUMN w String;
ALTER TABLE uk_ttl MODIFY COLUMN w String DEFAULT 'w';

DROP TABLE uk_ttl;

-- 2. ATTACH: red if the mutation guard admits MATERIALIZE TTL, or OPTIMIZE or its DRY RUN form
-- stops refusing a table with TTL.
DROP TABLE IF EXISTS uk_ttl_attach SYNC;
ATTACH TABLE uk_ttl_attach UUID '00000000-0000-0000-0000-000000004159'
(id UInt64, d Date, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
TTL d + INTERVAL 1 DAY;

ALTER TABLE uk_ttl_attach MATERIALIZE TTL; -- { serverError SUPPORT_IS_DISABLED }

INSERT INTO uk_ttl_attach VALUES (1, '2026-01-01', 'a');
INSERT INTO uk_ttl_attach VALUES (2, '2026-01-02', 'b');

OPTIMIZE TABLE uk_ttl_attach FINAL; -- { serverError SUPPORT_IS_DISABLED }

-- DRY RUN PARTS runs a real merge task, bypassing the OPTIMIZE guard, so it has its own.
OPTIMIZE TABLE uk_ttl_attach DRY RUN PARTS 'all_1_1_0', 'all_2_2_0'; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE uk_ttl_attach REMOVE TTL;

OPTIMIZE TABLE uk_ttl_attach FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'merged_after_remove_ttl' AS step, count() FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_ttl_attach' AND active;

DROP TABLE uk_ttl_attach SYNC;

-- 3. plain table: red if the CREATE-time TTL refusal applies to every MergeTree table.
DROP TABLE IF EXISTS plain_ttl;
CREATE TABLE plain_ttl (id UInt64, d Date) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY;
DROP TABLE plain_ttl;

SELECT 'ok' AS step;
