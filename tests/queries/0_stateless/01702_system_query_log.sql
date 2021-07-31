-- fire all kinds of queries and then check if those are present in the system.query_log
SET log_comment='system.query_log logging test';

SELECT 'DROP queries and also a cleanup before the test';
DROP DATABASE IF EXISTS sqllt SYNC;
DROP USER IF EXISTS sqllt_user;
DROP ROLE IF EXISTS sqllt_role;
DROP POLICY IF EXISTS sqllt_policy ON sqllt.table, sqllt.view, sqllt.dictionary;
DROP ROW POLICY IF EXISTS sqllt_row_policy ON sqllt.table, sqllt.view, sqllt.dictionary;
DROP QUOTA IF EXISTS sqllt_quota;
DROP SETTINGS PROFILE IF EXISTS sqllt_settings_profile;

SELECT 'CREATE queries';
CREATE DATABASE sqllt;

CREATE TABLE sqllt.table
(
    i UInt8, s String
)
ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple();

CREATE VIEW sqllt.view AS SELECT i, s FROM sqllt.table;
CREATE DICTIONARY sqllt.dictionary (key UInt64, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(DB 'sqllt' TABLE 'table' HOST 'localhost' PORT 9001)) LIFETIME(0) LAYOUT(FLAT());

CREATE USER sqllt_user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'password';
CREATE ROLE sqllt_role;

CREATE POLICY sqllt_policy ON sqllt.table, sqllt.view, sqllt.dictionary AS PERMISSIVE TO ALL;
CREATE POLICY sqllt_row_policy ON sqllt.table, sqllt.view, sqllt.dictionary AS PERMISSIVE TO ALL;

CREATE QUOTA sqllt_quota KEYED BY user_name TO sqllt_role;
CREATE SETTINGS PROFILE sqllt_settings_profile SETTINGS interactive_delay = 200000;

GRANT sqllt_role TO sqllt_user;


SELECT 'SET queries';
SET log_profile_events=false;
SET DEFAULT ROLE sqllt_role TO sqllt_user;
-- SET ROLE sqllt_role; -- tests are executed by user `default` which is defined in XML and is impossible to update.

SELECT 'ALTER TABLE queries';
ALTER TABLE sqllt.table ADD COLUMN new_col UInt32 DEFAULT 123456789;
ALTER TABLE sqllt.table COMMENT COLUMN new_col 'dummy column with a comment';
ALTER TABLE sqllt.table CLEAR COLUMN new_col;
ALTER TABLE sqllt.table MODIFY COLUMN new_col DateTime DEFAULT '2015-05-18 07:40:13';
ALTER TABLE sqllt.table MODIFY COLUMN new_col REMOVE COMMENT;
ALTER TABLE sqllt.table RENAME COLUMN new_col TO the_new_col;
ALTER TABLE sqllt.table DROP COLUMN the_new_col;
ALTER TABLE sqllt.table UPDATE i = i + 1 WHERE 1;
ALTER TABLE sqllt.table DELETE WHERE i > 65535;

-- not done, seems to hard, so I've skipped queries of ALTER-X, where X is:
-- PARTITION
-- ORDER BY
-- SAMPLE BY
-- INDEX
-- CONSTRAINT
-- TTL
-- USER
-- QUOTA
-- ROLE
-- ROW POLICY
-- SETTINGS PROFILE

SELECT 'SYSTEM queries';
SYSTEM FLUSH LOGS;
SYSTEM STOP MERGES sqllt.table;
SYSTEM START MERGES sqllt.table;
SYSTEM STOP TTL MERGES sqllt.table;
SYSTEM START TTL MERGES sqllt.table;
SYSTEM STOP MOVES sqllt.table;
SYSTEM START MOVES sqllt.table;
SYSTEM STOP FETCHES sqllt.table;
SYSTEM START FETCHES sqllt.table;
SYSTEM STOP REPLICATED SENDS sqllt.table;
SYSTEM START REPLICATED SENDS sqllt.table;

-- SYSTEM RELOAD DICTIONARY sqllt.dictionary; -- temporary out of order: Code: 210, Connection refused (localhost:9001) (version 21.3.1.1)
-- DROP REPLICA
-- haha, no
-- SYSTEM KILL;
-- SYSTEM SHUTDOWN;

-- Since we don't really care about the actual output, suppress it with `FORMAT Null`.
SELECT 'SHOW queries';

SHOW CREATE TABLE sqllt.table FORMAT Null;
SHOW CREATE DICTIONARY sqllt.dictionary FORMAT Null;
SHOW DATABASES LIKE 'sqllt' FORMAT Null;
SHOW TABLES FROM sqllt FORMAT Null;
SHOW DICTIONARIES FROM sqllt FORMAT Null;
SHOW GRANTS FORMAT Null;
SHOW GRANTS FOR sqllt_user FORMAT Null;
SHOW CREATE USER sqllt_user FORMAT Null;
SHOW CREATE ROLE sqllt_role FORMAT Null;
SHOW CREATE POLICY sqllt_policy FORMAT Null;
SHOW CREATE ROW POLICY sqllt_row_policy FORMAT Null;
SHOW CREATE QUOTA sqllt_quota FORMAT Null;
SHOW CREATE SETTINGS PROFILE sqllt_settings_profile FORMAT Null;

SELECT 'GRANT queries';
GRANT SELECT ON sqllt.table TO sqllt_user;
GRANT DROP ON sqllt.view TO sqllt_user;

SELECT 'REVOKE queries';
REVOKE SELECT ON sqllt.table FROM sqllt_user;
REVOKE DROP ON sqllt.view FROM sqllt_user;

SELECT 'Misc queries';
DESCRIBE TABLE sqllt.table FORMAT Null;

CHECK TABLE sqllt.table FORMAT Null;
DETACH TABLE sqllt.table;
ATTACH TABLE sqllt.table;

RENAME TABLE sqllt.table TO sqllt.table_new;
RENAME TABLE sqllt.table_new TO sqllt.table;
TRUNCATE TABLE sqllt.table;
DROP TABLE sqllt.table SYNC;

SET log_comment='';
---------------------------------------------------------------------------------------------------
-- Now get all logs related to this test
---------------------------------------------------------------------------------------------------

SYSTEM FLUSH LOGS;
SELECT 'ACTUAL LOG CONTENT:';

-- Try to filter out all possible previous junk events by excluding old log entries,
SELECT query_kind, query FROM system.query_log
WHERE
    log_comment LIKE '%system.query_log%' AND type == 'QueryStart' AND event_date >= yesterday()
    AND current_database == currentDatabase()
ORDER BY event_time_microseconds;


-- cleanup
SELECT 'DROP queries and also a cleanup after the test';
DROP DATABASE IF EXISTS sqllt;
DROP USER IF EXISTS sqllt_user;
DROP ROLE IF EXISTS sqllt_role;
DROP POLICY IF EXISTS sqllt_policy ON sqllt.table, sqllt.view, sqllt.dictionary;
DROP ROW POLICY IF EXISTS sqllt_row_policy ON sqllt.table, sqllt.view, sqllt.dictionary;
DROP QUOTA IF EXISTS sqllt_quota;
DROP SETTINGS PROFILE IF EXISTS sqllt_settings_profile;
