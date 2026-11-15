-- Companion to https://github.com/ClickHouse/ClickHouse/issues/105021.
--
-- Expected behavior (not yet implemented):
--   After `RENAME TABLE a TO c` of an `MV` source table within a single
--   database, the `MV` should keep working. `INSERT INTO c` should feed
--   `dst` through `mv` — either because the `MV`'s stored `SELECT … FROM a`
--   is rewritten to `FROM c` atomically with the rename (the same rewrite
--   `RENAME DATABASE` already performs), or because the catalog refuses to
--   rename a source table whose `MV` dependents would be orphaned. In any
--   case the silent data loss seen below is wrong from a user perspective.
--
-- Actual behavior (pinned by this test):
--   * The source-view edge in `system.tables.dependencies_table` correctly
--     re-keys from `a` to `c` (post-#98779 behavior).
--   * The `MV` is not dropped — it stays alive but orphaned. `SHOW CREATE`
--     and `SELECT * FROM mv` still work (the `MV` was created `TO dst`, so
--     reads go to the destination table directly). But `mv.as_select` still
--     references the now-dead name `default.a`.
--   * `InsertDependenciesBuilder::observePath` then compares
--     `mv.select_table_id = {default, a}` against the insert parent `{default, c}`,
--     decides `c` is not a source for `mv` anymore, and silently skips the
--     `MV`. `INSERT INTO c` ends up with `count(dst) = 1`, not the expected `2`.
--   * Recreating a brand-new table named `a` after the rename does NOT cause
--     the orphaned `MV` to latch onto it: the dependency edge in the catalog
--     graph is keyed at `c`, so `INSERT INTO` the new `a` finds no dependents
--     and does not fire the `MV`. This is the one piece of defensive behavior
--     the current state gets right — at least the orphaned `MV` cannot start
--     silently consuming data from a different, unrelated table that happens
--     to occupy the old name.
--
-- Why this hasn't been caught by the existing test suite:
--   * `00942_mv_rename_table` / `00943_mv_rename_without_inner_table` rename
--     the `MV`, not the source.
--   * `01155_rename_move_materialized_view` renames the source, but does so
--     across databases after `RENAME DATABASE` — which rewrites the `MV`'s
--     `select_table_id` to the new database name. By the time the
--     `RENAME TABLE` runs there, `observePath` already sees a matching name.
--
-- Status:
--   * Not a #98779 regression — the same end-user symptom (silent data loss)
--     exists on `v26.4.2.10` too, via the symmetric mechanism: pre-#98779
--     the source-view edge was not re-keyed at all, so `dependencies_table`
--     for `c` was empty and the `MV` was never even tried.
--   * Root cause is shared with #76303 (`RENAME TABLE` does not rewrite view
--     definitions). Fixing this case properly requires atomically rewriting
--     `MV.as_select` on rename and is out of scope of #105021's `EXCHANGE`
--     fix. This test pins the current buggy behavior so any future change
--     here is intentional.

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS c;

CREATE TABLE a (id Int32, key String) ENGINE = Null;
CREATE TABLE dst (id Int32, key String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT id, key FROM a;

-- Sanity: pre-rename, `INSERT INTO a` reaches `dst` via the `MV`.
INSERT INTO a VALUES (1, 'a-before');
SELECT 'before rename, count in dst', count() FROM dst;          -- expected: 1, actual: 1

SELECT 'before rename, deps of a', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'a';   -- ['mv']

RENAME TABLE a TO c;

-- Sanity: `a` is gone, `c` carries the source-view edge.
SELECT 'after rename, name exists for a',
       count() FROM system.tables WHERE database = currentDatabase() AND name = 'a';   -- 0
SELECT 'after rename, deps of c', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'c';   -- ['mv']

-- The `MV` is not dropped — it stays alive but orphaned.
SELECT 'after rename, mv still exists',
       count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv';   -- 1
SELECT 'after rename, mv as_select', as_select
FROM system.tables WHERE database = currentDatabase() AND name = 'mv';   -- SELECT id, key FROM default.a (dead reference)
SELECT 'after rename, select * from mv reads dst', id, key FROM mv ORDER BY id;   -- 1, 'a-before'

-- The user-visible symptom: `INSERT INTO c` should reach `dst` (expected: 2),
-- but `observePath` skips the `MV` because of the source-name mismatch above.
-- Currently `count(dst) = 1`. Update this reference when the latent bug is fixed.
INSERT INTO c VALUES (2, 'c-after');
SELECT 'after insert into c, count in dst', count() FROM dst;    -- expected: 2, actual: 1

-- Recreate a brand-new table named `a`. The catalog graph edge is keyed at `c`,
-- so the new `a` is not silently latched by the orphaned `MV`.
CREATE TABLE a (id Int32, key String) ENGINE = Null;
SELECT 'after recreating a, deps of new a', dependencies_table
FROM system.tables WHERE database = currentDatabase() AND name = 'a';   -- []

INSERT INTO a VALUES (99, 'new-a-after-recreate');
SELECT 'after insert into recreated a, count in dst', count() FROM dst;   -- 1 (unchanged — new `a` is unrelated)

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE a;
DROP TABLE c;
