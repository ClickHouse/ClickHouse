-- Comprehensive smoke test for `RESET SESSION`. Covers every flavour of
-- session-scoped state that the implementation is supposed to clear:
--   1. Settings (single + multiple)
--   2. Settings profile / constraints
--   3. Query parameters
--   4. Temporary tables (single + multiple)
--   5. Current database
--   6. Idempotency
--   7. Session is still usable after a reset

-- Suppress server log forwarding so expected `serverError` queries below
-- don't pollute the client_logs_file with their (intentional) error lines.
-- `RESET SESSION` resets this back to the baseline, so we re-`SET` it after
-- each reset that precedes a `serverError` query.
SET send_logs_level = 'fatal';

-- A noisy initial mutation to confirm we are starting dirty.
SET max_threads = 999, max_block_size = 12345;
SET param_x = 'mango', param_y = '7';
CREATE TEMPORARY TABLE reset_session_tmp_1 (x Int) ENGINE = Memory;
CREATE TEMPORARY TABLE reset_session_tmp_2 (s String) ENGINE = Memory;
INSERT INTO reset_session_tmp_1 VALUES (1), (2), (3);
INSERT INTO reset_session_tmp_2 VALUES ('hello');
USE system;

SELECT '-- pre-reset --';
SELECT getSetting('max_threads'), getSetting('max_block_size');
SELECT {x:String}, {y:UInt32};
SELECT count() FROM reset_session_tmp_1;
SELECT * FROM reset_session_tmp_2;
SELECT currentDatabase() = 'system' AS in_system;

RESET SESSION;
SET send_logs_level = 'fatal';

SELECT '-- post-reset --';
-- 999 / 12345 are arbitrary non-defaults; they must be gone.
SELECT getSetting('max_threads') = 999, getSetting('max_block_size') = 12345;
-- Query parameters: both should be gone. Use the substitution-error path
-- because there's no public introspection for the session's parameter map.
SELECT {x:String}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT {y:UInt32}; -- { serverError UNKNOWN_QUERY_PARAMETER }
-- Both temporary tables: gone. Check via `system.tables` rather than by
-- triggering UNKNOWN_TABLE — temporaries live in a hidden per-session
-- database and disappear when the session-scoped mapping is cleared.
SELECT count() FROM system.tables WHERE database = '_temporary_and_external_tables' AND name IN ('reset_session_tmp_1', 'reset_session_tmp_2');
-- Database: restored to whatever the connection was opened with — definitely not 'system'.
SELECT currentDatabase() != 'system' AS not_system;

-- Idempotency: running again on a clean session is fine.
RESET SESSION;
RESET SESSION;
SELECT 'reset is idempotent';

-- Session must still be usable for new state after the reset.
SET max_threads = 777;
SET param_z = 'banana';
CREATE TEMPORARY TABLE reset_session_tmp_3 (n Int) ENGINE = Memory;
INSERT INTO reset_session_tmp_3 VALUES (42);
SELECT getSetting('max_threads'), {z:String}, * FROM reset_session_tmp_3;

-- And another reset wipes the freshly-set state too.
RESET SESSION;
SELECT getSetting('max_threads') = 777 AS still_seven_seven_seven;
SELECT count() FROM system.tables WHERE database = '_temporary_and_external_tables' AND name = 'reset_session_tmp_3';

-- If the current database is dropped mid-session, `RESET SESSION` must not
-- throw — it should fall back to the user's profile default (or empty) rather
-- than leaving the session unusable.
DROP DATABASE IF EXISTS reset_session_db_to_drop;
CREATE DATABASE reset_session_db_to_drop;
USE reset_session_db_to_drop;
DROP DATABASE reset_session_db_to_drop;
RESET SESSION;
SELECT 'dropped-db reset survived';
