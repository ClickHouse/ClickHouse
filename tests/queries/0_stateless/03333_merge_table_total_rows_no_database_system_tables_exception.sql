-- Tags: no-replicated-database
-- ^ creates a database.

DROP DATABASE IF EXISTS {database}_extra;
CREATE DATABASE {database}_extra;
CREATE TABLE {database}_extra.t (x UInt8) ENGINE = Memory;
DROP TABLE IF EXISTS merge;
CREATE TABLE merge ENGINE = Merge({database}_extra, 't');
SELECT * FROM merge;
SELECT table, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND table = 'merge';
DROP DATABASE {database}_extra;
SELECT * FROM merge; -- { serverError UNKNOWN_DATABASE }
-- Even when the database behing the merge table does not exist anymore, querying the 'total_rows' field from system.tables does not throw an exception:
SELECT table, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND table = 'merge';
DROP TABLE merge;
