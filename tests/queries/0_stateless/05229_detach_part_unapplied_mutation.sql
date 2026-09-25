-- Tags: no-replicated-database
-- no-replicated-database: fails due to additional shard.

-- Plain (non-replicated) MergeTree DETACH PART, DETACH PARTITION and MOVE PARTITION TO TABLE must
-- reject a part that an unfinished mutation still has to rewrite. A mutation's done-ness is derived
-- from the active parts alone, so such a removal marks it done, and bringing the part back cannot
-- restore the obligation: an adopted part is renumbered above every mutation version, and both the
-- mutation assignment and the on-fly conversions select by that version alone. The acknowledged
-- ALTER would therefore never be applied to those rows, with no error and no failed mutation.
-- DROP and TRUNCATE destroy the data, so the mutation has nothing left to rewrite and stay allowed.
-- See #120902.

DROP TABLE IF EXISTS t_dpum_main;
DROP TABLE IF EXISTS t_dpum_dst;
DROP TABLE IF EXISTS t_dpum_drop;
DROP TABLE IF EXISTS t_dpum_scope;
DROP TABLE IF EXISTS t_dpum_nullable;
DROP TABLE IF EXISTS t_dpum_nullable_stats;
DROP TABLE IF EXISTS t_dpum_rename;
DROP TABLE IF EXISTS t_dpum_patch;

-- The last two are pinned because the stress runner randomizes both as client options, and every
-- block below asserts a read taken while a heavy mutation is deliberately still pending.
SET mutations_sync = 2;
SET alter_sync = 2;
SET alter_update_mode = 'heavy';
SET apply_mutations_on_fly = 0;

CREATE TABLE t_dpum_main (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_dpum_dst (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

-- 1. Arm: two parts and a mutation that stays pending because merges are stopped. The armed state is
-- asserted first, so the refusals below cannot pass vacuously against a mutation that already ended.
SYSTEM STOP MERGES t_dpum_main;
INSERT INTO t_dpum_main VALUES (1, 100), (2, 101);
INSERT INTO t_dpum_main VALUES (3, 200), (4, 201);
ALTER TABLE t_dpum_main UPDATE v = v + 1000 WHERE 1 SETTINGS mutations_sync = 0;
SELECT 'armed', is_done, parts_to_do FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_main';
SELECT 'armed parts', name FROM system.parts WHERE database = currentDatabase() AND table = 't_dpum_main' AND active ORDER BY name;

-- 2-4. The three commands that take a part out of the table while keeping its data recoverable.
ALTER TABLE t_dpum_main DETACH PART 'all_1_1_0'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_dpum_main DETACH PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_dpum_main MOVE PARTITION ID 'all' TO TABLE t_dpum_dst; -- { serverError SUPPORT_IS_DISABLED }

-- 5. Nothing half-happened: a refusal leaves the mutation, the parts and the data untouched.
SELECT 'after refusals', is_done, parts_to_do FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_main';
SELECT 'active parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_dpum_main' AND active;
SELECT 'detached parts', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_dpum_main';
SELECT 'sum', sum(v) FROM t_dpum_main;
SELECT 'dst rows', count() FROM t_dpum_dst;

-- 6. DROP control, on its own table so its block numbers cannot perturb the part names above: DROP
-- destroys the data, so it is still allowed with the very same pending mutation. This is the
-- boundary of the change and must not regress.
CREATE TABLE t_dpum_drop (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_dpum_drop;
INSERT INTO t_dpum_drop VALUES (1, 100), (2, 101);
INSERT INTO t_dpum_drop VALUES (3, 200), (4, 201);
ALTER TABLE t_dpum_drop UPDATE v = v + 1000 WHERE 1 SETTINGS mutations_sync = 0;
SELECT 'drop armed', is_done, parts_to_do FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_drop';
ALTER TABLE t_dpum_drop DROP PART 'all_1_1_0';
ALTER TABLE t_dpum_drop DROP PARTITION ID 'all';
SELECT 'drop allowed', count() FROM t_dpum_drop;

-- 7. A command the mutation executor would skip for a part is not an obligation of it: such a part
-- is cloned forward untouched, so the round trip loses nothing and refusing would be a false
-- refusal. Both halves are asserted: without the success half the filter is untested, without the
-- refusal half the block would pass with the guard disabled.
CREATE TABLE t_dpum_scope (p UInt64, id UInt64, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id;
SYSTEM STOP MERGES t_dpum_scope;
INSERT INTO t_dpum_scope VALUES (1, 1, 100), (1, 2, 101);
INSERT INTO t_dpum_scope VALUES (2, 3, 200), (2, 4, 201);
ALTER TABLE t_dpum_scope UPDATE v = v + 1000 IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 0;
SELECT 'scope armed', is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_scope';
ALTER TABLE t_dpum_scope DETACH PARTITION 2;
ALTER TABLE t_dpum_scope ATTACH PARTITION 2;
SELECT 'scope p2 allowed', sum(v) FROM t_dpum_scope WHERE p = 2;
ALTER TABLE t_dpum_scope DETACH PARTITION 1; -- { serverError SUPPORT_IS_DISABLED }

-- A MODIFY COLUMN to Nullable is a metadata-only conversion, and so skippable, only while the column
-- carries no statistics: statistics serialize differently for a nullable type and have to be
-- rewritten. Both table settings are pinned because the test runner randomizes both and both decide
-- which branch this is: `auto_statistics_types` attaches statistics to the column, and a sparse
-- column is not metadata-only convertible either.
CREATE TABLE t_dpum_nullable (p UInt64, id UInt64, w UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1.0;
SYSTEM STOP MERGES t_dpum_nullable;
INSERT INTO t_dpum_nullable VALUES (1, 1, 100), (1, 2, 101);
INSERT INTO t_dpum_nullable VALUES (2, 3, 200), (2, 4, 201);
ALTER TABLE t_dpum_nullable MODIFY COLUMN w Nullable(UInt64) SETTINGS alter_sync = 0;
SELECT 'nullable armed', is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_nullable';
ALTER TABLE t_dpum_nullable DETACH PARTITION 2;
ALTER TABLE t_dpum_nullable ATTACH PARTITION 2;
SELECT 'nullable allowed', sum(w) FROM t_dpum_nullable WHERE p = 2;

CREATE TABLE t_dpum_nullable_stats (p UInt64, id UInt64, w UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS auto_statistics_types = 'basic', ratio_of_defaults_for_sparse_serialization = 1.0;
SYSTEM STOP MERGES t_dpum_nullable_stats;
INSERT INTO t_dpum_nullable_stats VALUES (1, 1, 100), (1, 2, 101);
INSERT INTO t_dpum_nullable_stats VALUES (2, 3, 200), (2, 4, 201);
ALTER TABLE t_dpum_nullable_stats MODIFY COLUMN w Nullable(UInt64) SETTINGS alter_sync = 0;
SELECT 'nullable stats armed', is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_nullable_stats';
ALTER TABLE t_dpum_nullable_stats DETACH PARTITION 2; -- { serverError SUPPORT_IS_DISABLED }

-- 8. No carve-out by command kind. A pending RENAME COLUMN is lost by the same mechanism: after a
-- detach/attach round trip the renamed column reads back as defaults.
CREATE TABLE t_dpum_rename (id UInt64, w UInt64) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_dpum_rename;
INSERT INTO t_dpum_rename VALUES (1, 100), (2, 101);
ALTER TABLE t_dpum_rename RENAME COLUMN w TO w2 SETTINGS alter_sync = 0;
SELECT 'rename armed', is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_rename';
ALTER TABLE t_dpum_rename DETACH PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

-- 9. The way out that the error message offers: once the mutation is materialized the same commands
-- are allowed again, and the round trip keeps the updated values. The barrier UPDATE inherits
-- mutations_sync = 2, and a later mutation cannot finish before an earlier one, so waiting for it
-- waits for both. PARTITION rather than PART, because post-mutation part names depend on how many
-- block numbers the statements above consumed.
SYSTEM START MERGES t_dpum_main;
ALTER TABLE t_dpum_main UPDATE v = v WHERE 0;
SELECT 'migrated', min(is_done), count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_main';
ALTER TABLE t_dpum_main DETACH PARTITION ID 'all';
ALTER TABLE t_dpum_main ATTACH PARTITION ID 'all';
SELECT 'migrated sum', sum(v) FROM t_dpum_main;

-- 10. The neighbouring patch guard covers lightweight updates, whose obligation lives in a patch
-- part rather than in a mutation entry. A patch keeps being applied on read after lightweight
-- updates are switched off again, so switching them off must not let the base part leave the table.
-- Both block-number settings are pinned in the CREATE because the test runner randomizes them, and
-- the refusal below cannot come from the mutation guard: this table has no pending mutation at all.
CREATE TABLE t_dpum_patch (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_dpum_patch;
INSERT INTO t_dpum_patch VALUES (1, 10);
ALTER TABLE t_dpum_patch UPDATE v = v + 100 WHERE 1 SETTINGS enable_lightweight_update = 1, alter_update_mode = 'lightweight_force';
ALTER TABLE t_dpum_patch MODIFY SETTING enable_block_number_column = 0;
-- The patch still applies on read here, which is the obligation the base part may not take away.
SELECT 'patch applied', sum(v) FROM t_dpum_patch;
-- Lightweight updates are off now, which is what makes the two refusals below the guard's own.
ALTER TABLE t_dpum_patch UPDATE v = v + 1 WHERE 1 SETTINGS enable_lightweight_update = 1, alter_update_mode = 'lightweight_force'; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'patch armed', countIf(startsWith(name, 'patch-')) FROM system.parts WHERE database = currentDatabase() AND table = 't_dpum_patch' AND active;
SELECT 'patch pending mutations', countIf(NOT is_done) FROM system.mutations WHERE database = currentDatabase() AND table = 't_dpum_patch';
ALTER TABLE t_dpum_patch DETACH PART 'all_1_1_0'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_dpum_patch DETACH PARTITION ID 'all'; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_dpum_main;
DROP TABLE t_dpum_dst;
DROP TABLE t_dpum_drop;
DROP TABLE t_dpum_scope;
DROP TABLE t_dpum_nullable;
DROP TABLE t_dpum_nullable_stats;
DROP TABLE t_dpum_rename;
DROP TABLE t_dpum_patch;
