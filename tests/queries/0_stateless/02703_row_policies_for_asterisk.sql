-- Tags: no-parallel

SELECT 'Policy for table `*` does not affect other tables in the database';
CREATE DATABASE 02703_db_asterisk;
CREATE ROW POLICY 02703_asterisk ON 02703_db_asterisk.`*` USING x=1 AS permissive TO ALL;
CREATE TABLE 02703_db_asterisk.`*` (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 100, 20;
CREATE TABLE 02703_db_asterisk.`other` (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 100, 20;
SELECT 'star', * FROM 02703_db_asterisk.`*`;
SELECT 'other', * FROM 02703_db_asterisk.other;
DROP ROW POLICY 02703_asterisk ON 02703_db_asterisk.`*`;
DROP DATABASE 02703_db_asterisk;
