-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-fasttest
-- UNIQUE KEY DELETE:
--   1. explicit transaction: a DELETE inside a transaction is refused and changes nothing
--   2. Alias: a DELETE through an Alias table deletes from the table it points to
--   3. shadowed virtual column: a table column named `_part_offset`, `_part` or `_partition_id` refuses the DELETE
--   4. lazy table: a DELETE reaches a table whose database loads it lazily, directly and through an Alias

SET enable_unique_key = 1;
SET allow_experimental_alias_table_engine = 1;

-- 1. explicit transaction: red if a UNIQUE KEY write stops refusing an explicit transaction (the
-- DELETE then nests in the implicit transaction and aborts the debug server).
DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_guard;

INSERT INTO uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

DELETE FROM uk_guard WHERE id % 2 = 0;  -- removes 2, 4

-- NOT_IMPLEMENTED where transactions are off (the default), SUPPORT_IS_DISABLED where they are on.
DELETE FROM uk_guard WHERE id = 1 SETTINGS implicit_transaction = 1; -- { serverError SUPPORT_IS_DISABLED, NOT_IMPLEMENTED }

SELECT 'survivors_unchanged' AS step, id FROM uk_guard ORDER BY id;  -- 1,3,5

DROP TABLE uk_guard;

-- A DELETE with no partition to visit is refused too.
DROP TABLE IF EXISTS uk_guard_empty;

CREATE TABLE uk_guard_empty (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

DELETE FROM uk_guard_empty WHERE id = 1 SETTINGS implicit_transaction = 1; -- { serverError SUPPORT_IS_DISABLED, NOT_IMPLEMENTED }

DROP TABLE uk_guard_empty;

-- 2. Alias: red if the DELETE through the alias is refused (SUPPORT_IS_DISABLED) or leaves row 2.
DROP TABLE IF EXISTS uk_alias;
DROP TABLE IF EXISTS uk_alias_target;

CREATE TABLE uk_alias_target (id UInt64) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id);
CREATE TABLE uk_alias ENGINE = Alias('uk_alias_target');

INSERT INTO uk_alias_target VALUES (1), (2), (3);

DELETE FROM uk_alias WHERE id = 2;

SELECT 'alias_delete' AS step, id FROM uk_alias_target ORDER BY id;  -- 1,3

DROP TABLE uk_alias;
DROP TABLE uk_alias_target;

-- 3. shadowed virtual column: red if the DELETE runs, taking the stored `_part_offset` as the row address
-- (deleting id 2 then kills id 1), crashing on a stored `_part`, or taking the stored `_partition_id` as
-- the partition (deleting partition 0 then kills id 2 in partition 1).
DROP TABLE IF EXISTS uk_shadow_offset;
DROP TABLE IF EXISTS uk_shadow_part;
DROP TABLE IF EXISTS uk_shadow_partition;

CREATE TABLE uk_shadow_offset (id UInt64, _part_offset UInt64) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id);
CREATE TABLE uk_shadow_part (id UInt64, _part String) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id);
CREATE TABLE uk_shadow_partition (id UInt64, p UInt8, _partition_id String) ENGINE = MergeTree PARTITION BY p UNIQUE KEY (id) ORDER BY (id);

INSERT INTO uk_shadow_offset VALUES (1, 0), (2, 0);
INSERT INTO uk_shadow_part VALUES (1, 'x'), (2, 'x');
INSERT INTO uk_shadow_partition VALUES (1, 0, '0'), (2, 1, '0');

DELETE FROM uk_shadow_offset WHERE id = 2; -- { serverError ILLEGAL_COLUMN }
DELETE FROM uk_shadow_part WHERE id = 2; -- { serverError ILLEGAL_COLUMN }
DELETE FROM uk_shadow_partition IN PARTITION 0 WHERE 1; -- { serverError ILLEGAL_COLUMN }

SELECT 'shadow_offset' AS step, id FROM uk_shadow_offset ORDER BY id;  -- 1,2
SELECT 'shadow_part' AS step, id FROM uk_shadow_part ORDER BY id;  -- 1,2
SELECT 'shadow_partition' AS step, id FROM uk_shadow_partition ORDER BY id;  -- 1,2

DROP TABLE uk_shadow_offset;
DROP TABLE uk_shadow_part;
DROP TABLE uk_shadow_partition;

-- 4. lazy table: red if the DELETE goes to the catalog's stand-in, which refuses it (SUPPORT_IS_DISABLED).
-- Each DELETE follows a re-attach, which is what puts a stand-in in the catalog.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.uk_lazy (id UInt64) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id);
CREATE TABLE uk_lazy_alias ENGINE = Alias({CLICKHOUSE_DATABASE_1:String}, 'uk_lazy');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.uk_lazy VALUES (1), (2), (3);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'lazy_stand_in' AS step, engine FROM system.tables WHERE database = currentDatabase() || '_1' AND name = 'uk_lazy';
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.uk_lazy WHERE id = 1;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DELETE FROM uk_lazy_alias WHERE id = 2;

SELECT 'lazy_survivors' AS step, id FROM {CLICKHOUSE_DATABASE_1:Identifier}.uk_lazy ORDER BY id;  -- 3

DROP TABLE uk_lazy_alias;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
