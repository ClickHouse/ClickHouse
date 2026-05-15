-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104791
--
-- `ATTACH TABLE name <storage clauses>;` without `ENGINE` and without a columns list
-- used to silently drop the user-provided storage clauses, because the short ATTACH
-- path reads the table definition from stored metadata and overwrites anything the
-- user typed. The parser allows `SETTINGS`, `ORDER BY`, `PARTITION BY`, etc. without
-- an `ENGINE` to support `default_table_engine` on CREATE, but for ATTACH the same
-- syntax produced no error and no effect — users assumed their settings took effect.
-- We now reject this with `BAD_ARGUMENTS` so the user notices.

SET default_table_engine = 'MergeTree';

DROP TABLE IF EXISTS t_104791;
CREATE TABLE t_104791 (id UInt32) ORDER BY id;

-- 1. SETTINGS without ENGINE: should error (the original bug from the issue).
DETACH TABLE t_104791;
ATTACH TABLE t_104791 SETTINGS max_part_loading_threads = 16; -- { serverError BAD_ARGUMENTS }

-- 2. ORDER BY without ENGINE: same bug class, same fix.
ATTACH TABLE t_104791 ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- 3. PARTITION BY without ENGINE: same.
ATTACH TABLE t_104791 PARTITION BY id; -- { serverError BAD_ARGUMENTS }

-- 4. Short ATTACH (no storage clauses at all): must still work.
ATTACH TABLE t_104791;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_104791';

DROP TABLE t_104791;
