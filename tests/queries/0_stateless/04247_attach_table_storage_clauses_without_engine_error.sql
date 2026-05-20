-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104791
--
-- `ATTACH TABLE name <storage clauses>;` without `ENGINE` and without a columns list
-- used to silently drop the user-provided storage clauses, because the short ATTACH
-- path reads the table definition from stored metadata and overwrites anything the
-- user typed. The parser allows `SETTINGS`, `ORDER BY`, `PARTITION BY`, etc. without
-- an `ENGINE` to support `default_table_engine` on `CREATE`, but for `ATTACH` the
-- same syntax produced no error and no effect, and users assumed their settings took
-- effect. We now reject this with `BAD_ARGUMENTS` so the user notices.
--
-- However `ATTACH TABLE t SETTINGS log_comment = 'foo';` is a legitimate, supported
-- pattern: `InterpreterSetQuery::applySettingsFromQuery` extracts session settings
-- out of `create.storage->settings` and applies them to the context BEFORE this
-- interpreter runs, so by the time the guard sees the storage, those settings have
-- already been hoisted out and `storage->settings` is reset. The guard must allow
-- that case.
--
-- The guard also covers top-level `ASTCreateQuery` fields (`COMMENT`, `EMPTY`,
-- `CLONE`, `REFRESH`, `TO target`, `AS SELECT`, etc.) that the parser may attach to
-- an `ATTACH` query but that the short `ATTACH` path silently drops by overwriting
-- the user-provided `create` with stored metadata.

SET default_table_engine = 'MergeTree';

DROP TABLE IF EXISTS t_104791;
CREATE TABLE t_104791 (id UInt32) ORDER BY id;

-- 1. SETTINGS with an engine (MergeTree) setting and no ENGINE: should error.
--    `max_part_loading_threads` is neither a session setting nor a builtin
--    MergeTree setting, so `applySettingsFromQuery` leaves it inside
--    `storage->settings`. The guard catches it.
DETACH TABLE t_104791;
ATTACH TABLE t_104791 SETTINGS max_part_loading_threads = 16; -- { serverError BAD_ARGUMENTS }

-- 2. SETTINGS with a MergeTree-builtin setting (`index_granularity`): should error.
--    `applySettingsFromQuery` keeps MergeTree builtin settings inside
--    `storage->settings`.
ATTACH TABLE t_104791 SETTINGS index_granularity = 100; -- { serverError BAD_ARGUMENTS }

-- 3. ORDER BY without ENGINE: same bug class, same fix.
ATTACH TABLE t_104791 ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- 4. PARTITION BY without ENGINE: same.
ATTACH TABLE t_104791 PARTITION BY id; -- { serverError BAD_ARGUMENTS }

-- 5. Mixed (session + MergeTree builtin) SETTINGS: must still error, because
--    after `applySettingsFromQuery` extracts `log_comment`, the MergeTree
--    builtin setting `index_granularity` remains in `storage->settings`.
ATTACH TABLE t_104791 SETTINGS log_comment = 'attach_test_104791', index_granularity = 100; -- { serverError BAD_ARGUMENTS }

-- 6. Session-only SETTINGS (`log_comment`): must succeed. `applySettingsFromQuery`
--    hoists `log_comment` to the context and resets `storage->settings`, so the
--    guard sees an empty storage and falls through to the short-ATTACH path.
ATTACH TABLE t_104791 SETTINGS log_comment = 'attach_test_104791';
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_104791';

-- 7. Short ATTACH (no storage clauses at all): must still work.
DETACH TABLE t_104791;
ATTACH TABLE t_104791;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_104791';

-- 8. Top-level COMMENT on a short ATTACH: silently dropped before this patch,
--    now rejected. The user-supplied comment would not have replaced the stored
--    one because the short-ATTACH path overwrites `create.comment` from metadata.
DETACH TABLE t_104791;
ATTACH TABLE t_104791 COMMENT 'this comment would be silently dropped'; -- { serverError BAD_ARGUMENTS }

-- 9. EMPTY AS SELECT on a short ATTACH: silently dropped before this patch.
ATTACH TABLE t_104791 EMPTY AS SELECT 1; -- { serverError BAD_ARGUMENTS }

-- 10. CLONE AS source on a short ATTACH: silently dropped before this patch.
ATTACH TABLE t_104791 CLONE AS t_104791; -- { serverError BAD_ARGUMENTS }

-- 11. Top-level UUID on a short ATTACH: silently dropped before this patch
--     (the stored UUID overwrites the user-supplied one when `create` is replaced
--     by stored metadata in the short-ATTACH path below).
ATTACH TABLE t_104791 UUID '00000000-0000-0000-0000-000000000001'; -- { serverError BAD_ARGUMENTS }

-- 12. Explicit `Nil` UUID on a short ATTACH: also silently dropped before this
--     patch. `ParserCompoundIdentifier` parses `UUID '...'` into `table_id->uuid`,
--     and `Nil == Nil` so the value-based check we used previously missed this
--     case. The fix is the `has_uuid_clause` flag on `ASTTableIdentifier`, which
--     records that the parser actually saw the `UUID '...'` clause regardless of
--     value, and which propagates into `ASTCreateQuery::has_uuid_clause`.
ATTACH TABLE t_104791 UUID '00000000-0000-0000-0000-000000000000'; -- { serverError BAD_ARGUMENTS }

-- 13. `FROM '/path'` on an `ATTACH TABLE`: silently dropped before this patch.
--     The parser sets `create.has_attach_from_path` and `create.attach_from_path`,
--     but `create = create_query` in the short-ATTACH path replaces both with the
--     stored AST. The legitimate use of `FROM` is `ATTACH TABLE t (columns) FROM
--     '/path' ENGINE = ...` (long ATTACH), which has a columns list and so is
--     not affected by this guard.
ATTACH TABLE t_104791 FROM '/var/lib/clickhouse/detached/t_104791'; -- { serverError BAD_ARGUMENTS }

-- Restore the table so cleanup at the end works.
ATTACH TABLE t_104791;
DROP TABLE t_104791;

-- Materialized view cases. The parser accepts `ATTACH MATERIALIZED VIEW t ...`
-- with REFRESH, TO target, and AS SELECT clauses; before this patch all of
-- them were silently dropped by the short-ATTACH path. The guard now rejects
-- them so the user has to use `ATTACH MATERIALIZED VIEW t;` or `ALTER TABLE
-- t MODIFY REFRESH ...` instead.
DROP TABLE IF EXISTS t_mv_104791;
DROP TABLE IF EXISTS t_mv_104791_target;
DROP TABLE IF EXISTS t_mv_104791_other_target;

CREATE TABLE t_mv_104791_target (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_mv_104791_other_target (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t_mv_104791 REFRESH EVERY 1 HOUR TO t_mv_104791_target AS SELECT 1 AS id, 2 AS x;
DETACH TABLE t_mv_104791;

-- 14. ATTACH MV with a different REFRESH schedule: silently dropped before this
--     patch (the stored REFRESH EVERY 1 HOUR replaced the user's EVERY 1 YEAR).
ATTACH MATERIALIZED VIEW t_mv_104791 REFRESH EVERY 1 YEAR TO t_mv_104791_target AS SELECT 5 AS id, 6 AS x; -- { serverError BAD_ARGUMENTS }

-- 15. ATTACH MV with a different TO target: silently dropped before this patch.
ATTACH MATERIALIZED VIEW t_mv_104791 TO t_mv_104791_other_target AS SELECT 5 AS id, 6 AS x; -- { serverError BAD_ARGUMENTS }

-- 16. ATTACH MV with a different AS SELECT: silently dropped before this patch.
ATTACH MATERIALIZED VIEW t_mv_104791 TO t_mv_104791_target AS SELECT 5 AS id, 6 AS x; -- { serverError BAD_ARGUMENTS }

-- Restore the MV so cleanup works.
ATTACH TABLE t_mv_104791;
DROP TABLE t_mv_104791;
DROP TABLE t_mv_104791_target;
DROP TABLE t_mv_104791_other_target;
