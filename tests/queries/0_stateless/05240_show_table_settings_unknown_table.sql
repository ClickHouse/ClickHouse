-- `SHOW TABLE SETTINGS` on a table that does not exist is an error, as `SHOW CREATE TABLE` is, rather than an
-- empty list that would read as a table with no settings changed.

DROP TABLE IF EXISTS no_such_table_05240;

SHOW TABLE SETTINGS FROM no_such_table_05240; -- { serverError UNKNOWN_TABLE }
SHOW CHANGED TABLE SETTINGS FROM no_such_table_05240; -- { serverError UNKNOWN_TABLE }
SHOW TABLE SETTINGS FROM system.no_such_table_05240; -- { serverError UNKNOWN_TABLE }
SHOW TABLE SETTINGS FROM no_such_database_05240.t; -- { serverError UNKNOWN_DATABASE }

SELECT '-- a temporary table is still found';
CREATE TEMPORARY TABLE tmp_05240 (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 10;
SHOW TABLE SETTINGS FROM tmp_05240 LIKE 'max_rows_to_keep';
