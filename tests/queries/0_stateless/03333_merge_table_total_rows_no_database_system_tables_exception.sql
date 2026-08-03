-- Tags: no-parallel, no-replicated-database
-- ^ creates a database.

DROP DATABASE IF EXISTS test_03333;
CREATE DATABASE test_03333;
CREATE TABLE test_03333.t (x UInt8) ENGINE = Memory;
DROP TABLE IF EXISTS merge;
CREATE TABLE merge ENGINE = Merge(test_03333, 't');
SELECT * FROM merge;
SELECT table, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND table = 'merge';
DROP DATABASE test_03333;
SELECT * FROM merge; -- { serverError UNKNOWN_DATABASE }
-- Even when the database behing the merge table does not exist anymore, querying the 'total_rows' field from system.tables does not throw an exception:
SELECT table, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND table = 'merge';
DROP TABLE merge;
