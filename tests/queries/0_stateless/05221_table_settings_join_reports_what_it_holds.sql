-- `Join` keeps no settings object. Its creator resolves eight settings once - from the table's own `SETTINGS`
-- clause and, for what the clause leaves out, from the server's settings - and passes the results to the
-- storage. `system.table_settings` reports the values the table holds, so a setting the clause leaves out is
-- still visible, although `SHOW CREATE TABLE` never shows it.

DROP TABLE IF EXISTS join_stated;
DROP TABLE IF EXISTS join_session_0;
DROP TABLE IF EXISTS join_session_1;

-- An `ALL` join, so `any_join_distinct_right_table_keys` has no effect here and the storage's strictness cannot
-- say what it was.
CREATE TABLE join_stated (k UInt64, v UInt64) ENGINE = Join(ALL, LEFT, k)
    SETTINGS join_use_nulls = 1, join_overflow_mode = 'break', any_join_distinct_right_table_keys = 1, persistent = 0;

SELECT '-- all eight settings are reported';
SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 'join_stated';

SELECT '-- what the clause states is reported with its value, as the definition';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'join_stated' AND source = 'definition'
ORDER BY name;

SELECT '-- what the clause leaves out is reported with the value the server supplies';
-- Against `system.settings` rather than against fixed values, because the server's profile decides them: the
-- stateless configuration sets `max_rows_in_join` and `max_bytes_in_join` in its default profile, while a bare
-- server leaves them at the compiled-in defaults. This is the assertion that would catch an implementation
-- reporting the compiled-in default instead of the value the table actually holds.
SELECT ts.name, ts.value = s.value AS holds_the_server_value, ts.source IN ('default', 'other') AS source_is_expected
FROM system.table_settings AS ts
INNER JOIN system.settings AS s ON s.name = ts.name
WHERE ts.database = currentDatabase() AND ts.table = 'join_stated' AND ts.source != 'definition'
ORDER BY ts.name;

-- `disk` and `persistent` have no server setting behind them, so they are not in the join above.
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'join_stated' AND name = 'disk';

SELECT '-- the creating session does not supply those values: the server does';
SET join_use_nulls = 0;
CREATE TABLE join_session_0 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
SET join_use_nulls = 1;
CREATE TABLE join_session_1 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
SELECT uniqExact(value) FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('join_session_0', 'join_session_1') AND name = 'join_use_nulls';

-- Not an assertion about this feature: it records that the stored definition is where the value is *not*
-- visible, which is the reason the table has to report it.
SELECT '-- and none of it is in the stored definition';
SELECT create_table_query LIKE '%SETTINGS%' FROM system.tables
WHERE database = currentDatabase() AND name = 'join_session_1';

SELECT '-- the values reported are the ones the engine acts on';
DROP TABLE IF EXISTS join_limited;
DROP TABLE IF EXISTS join_last_row;
CREATE TABLE join_limited (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k)
    SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw';
SELECT name, value FROM system.table_settings
WHERE database = currentDatabase() AND table = 'join_limited' AND name = 'max_rows_in_join';
INSERT INTO join_limited VALUES (1, 1), (2, 2), (3, 3); -- { serverError SET_SIZE_LIMIT_EXCEEDED }
CREATE TABLE join_last_row (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_any_take_last_row = 1;
INSERT INTO join_last_row VALUES (1, 10);
INSERT INTO join_last_row VALUES (1, 20);
-- The row the engine returns, and the value reported for the setting that decides it.
SELECT joinGet('join_last_row', 'v', toUInt64(1));
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'join_last_row' AND name = 'join_any_take_last_row';

SELECT '-- a temporary table reports its definition too';
CREATE TEMPORARY TABLE join_temporary (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 0;
SELECT name, value, source FROM system.table_settings
WHERE database = '' AND table = 'join_temporary' AND name = 'persistent';

SELECT '-- `system.engine_settings` lists the same eight, as a table created now would get them';
SELECT count() FROM system.engine_settings WHERE engine = 'Join';
-- A table that states nothing reports exactly what the engine-level rows say.
SELECT count() FROM (
    SELECT name, value, `default`, changed, description, type, tier FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'join_session_0'
    EXCEPT
    SELECT name, value, `default`, changed, description, type, tier FROM system.engine_settings WHERE engine = 'Join');

SELECT '-- and `Join` and `Set` describe the two settings they share alike';
SELECT count() FROM (
    SELECT name, `default`, description, type FROM system.engine_settings WHERE engine = 'Join' AND name IN ('disk', 'persistent')
    EXCEPT
    SELECT name, `default`, description, type FROM system.engine_settings WHERE engine = 'Set' AND name IN ('disk', 'persistent'));

DROP TABLE join_stated;
DROP TABLE join_session_0;
DROP TABLE join_session_1;
DROP TABLE join_limited;
DROP TABLE join_last_row;
