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

SELECT '-- what the clause leaves out is reported too, and claims `default` only when it is the default';
-- Not the values themselves: the server's own settings supply them, and a test server may change some. The
-- stateless configuration sets `max_rows_in_join` and `max_bytes_in_join` in its default profile, and those then
-- report `other`.
SELECT name, (value = `default`) = (source = 'default') AS source_matches_value, source IN ('default', 'other') AS source_is_expected
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'join_stated' AND source != 'definition'
ORDER BY name;

SELECT '-- the creating session does not supply those values: the server does';
SET join_use_nulls = 0;
CREATE TABLE join_session_0 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
SET join_use_nulls = 1;
CREATE TABLE join_session_1 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
SELECT uniqExact(value) FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('join_session_0', 'join_session_1') AND name = 'join_use_nulls';

SELECT '-- and none of it is in the stored definition';
SELECT create_table_query LIKE '%SETTINGS%' FROM system.tables
WHERE database = currentDatabase() AND name = 'join_session_1';

DROP TABLE join_stated;
DROP TABLE join_session_0;
DROP TABLE join_session_1;
