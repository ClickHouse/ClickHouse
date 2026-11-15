-- Tags: no-replicated-database, memory-engine
-- Tag no-replicated-database: Unsupported type of CREATE TABLE ... CLONE AS ... query

DROP TABLE IF EXISTS foo_memory;
DROP TABLE IF EXISTS clone_as_foo_memory;
DROP TABLE IF EXISTS foo_file;
DROP TABLE IF EXISTS clone_as_foo_file;
DROP TABLE IF EXISTS foo_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_merge_tree_p_x;
DROP TABLE IF EXISTS clone_as_foo_merge_tree_p_y;
DROP TABLE IF EXISTS foo_replacing_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree_p_x;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree_p_y;
DROP TABLE IF EXISTS foo_replicated_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree_p_x;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree_p_y;

-- CLONE AS with a table of Memory engine
CREATE TABLE foo_memory (x Int8, y String) ENGINE=Memory;
SHOW CREATE TABLE foo_memory;
INSERT INTO foo_memory VALUES (1, 'a'), (2, 'b');

CREATE TABLE clone_as_foo_memory CLONE AS foo_memory; -- { serverError SUPPORT_IS_DISABLED } 

-- CLONE AS with a table of File engine
CREATE TABLE foo_file (x Int8, y String) ENGINE=File(TabSeparated);
SHOW CREATE TABLE foo_file;
INSERT INTO foo_file VALUES (1, 'a'), (2, 'b');

CREATE TABLE clone_as_foo_file CLONE AS foo_file; -- { serverError SUPPORT_IS_DISABLED } 

-- CLONE AS with a table of MergeTree engine
CREATE TABLE foo_merge_tree (x Int8, y String) ENGINE=MergeTree PRIMARY KEY x;
SHOW CREATE TABLE foo_merge_tree;
INSERT INTO foo_merge_tree VALUES (1, 'a'), (2, 'b');
SELECT * FROM foo_merge_tree;

CREATE TABLE clone_as_foo_merge_tree CLONE AS foo_merge_tree;
SHOW CREATE TABLE clone_as_foo_merge_tree;
SELECT 'from foo_merge_tree';
SELECT * FROM foo_merge_tree;
SELECT 'from clone_as_foo_merge_tree';
SELECT * FROM clone_as_foo_merge_tree;

-- Specify ENGINE
CREATE TABLE clone_as_foo_merge_tree_p_x CLONE AS foo_merge_tree ENGINE=MergeTree PRIMARY KEY x;
SELECT 'from clone_as_foo_merge_tree_p_x';
SELECT * FROM clone_as_foo_merge_tree_p_x;
CREATE TABLE clone_as_foo_merge_tree_p_y CLONE AS foo_merge_tree ENGINE=MergeTree PRIMARY KEY y; -- { serverError BAD_ARGUMENTS }

-- CLONE AS with a table of ReplacingMergeTree engine
CREATE TABLE foo_replacing_merge_tree (x Int8, y String) ENGINE=ReplacingMergeTree PRIMARY KEY x;
SHOW CREATE TABLE foo_replacing_merge_tree;
INSERT INTO foo_replacing_merge_tree VALUES (1, 'a'), (2, 'b');
SELECT * FROM foo_replacing_merge_tree;

CREATE TABLE clone_as_foo_replacing_merge_tree CLONE AS foo_replacing_merge_tree;
SHOW CREATE TABLE clone_as_foo_replacing_merge_tree;
SELECT 'from foo_replacing_merge_tree';
SELECT * FROM foo_replacing_merge_tree;
SELECT 'from clone_as_foo_replacing_merge_tree';
SELECT * FROM clone_as_foo_replacing_merge_tree;

-- Specify ENGINE
CREATE TABLE clone_as_foo_replacing_merge_tree_p_x CLONE AS foo_replacing_merge_tree ENGINE=ReplacingMergeTree PRIMARY KEY x;
SELECT 'from clone_as_foo_replacing_merge_tree_p_x';
SELECT * FROM clone_as_foo_replacing_merge_tree_p_x;
CREATE TABLE clone_as_foo_replacing_merge_tree_p_y CLONE AS foo_replacing_merge_tree ENGINE=ReplacingMergeTree PRIMARY KEY y; -- { serverError BAD_ARGUMENTS }

-- CLONE AS with a table of ReplicatedMergeTree engine
CREATE TABLE foo_replicated_merge_tree (x Int8, y String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_foo_replicated_merge_tree', 'r1') PRIMARY KEY x;
SHOW CREATE TABLE foo_replicated_merge_tree;
INSERT INTO foo_replicated_merge_tree VALUES (1, 'a'), (2, 'b');
SELECT 'from foo_replicated_merge_tree';
SELECT * FROM foo_replicated_merge_tree;

CREATE TABLE clone_as_foo_replicated_merge_tree CLONE AS foo_replicated_merge_tree; -- { serverError REPLICA_ALREADY_EXISTS }

-- Specify ENGINE
CREATE TABLE clone_as_foo_replicated_merge_tree_p_x CLONE AS foo_replicated_merge_tree ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/clone_as_foo_replicated_merge_tree_p_x', 'r1') PRIMARY KEY x;
SHOW CREATE TABLE clone_as_foo_replicated_merge_tree_p_x;
SELECT 'from clone_as_foo_replicated_merge_tree_p_x';
SELECT * FROM foo_replicated_merge_tree;
CREATE TABLE clone_as_foo_replicated_merge_tree_p_y CLONE AS foo_replicated_merge_tree ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/clone_as_foo_replicated_merge_tree_p_y', 'r1') PRIMARY KEY y; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS foo_memory;
DROP TABLE IF EXISTS clone_as_foo_memory;
DROP TABLE IF EXISTS foo_file;
DROP TABLE IF EXISTS clone_as_foo_file;
DROP TABLE IF EXISTS foo_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_merge_tree_p_x;
DROP TABLE IF EXISTS clone_as_foo_merge_tree_p_y;
DROP TABLE IF EXISTS foo_replacing_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree_p_x;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree_p_y;
DROP TABLE IF EXISTS foo_replicated_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree_p_x;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree_p_y;

-- CLONE AS with a Replicated database
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/databases/{database}/test_03231', 's1', 'r1');
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE foo_merge_tree (x Int8, y String) ENGINE=MergeTree PRIMARY KEY x;
SHOW CREATE TABLE foo_merge_tree;
INSERT INTO foo_merge_tree VALUES (1, 'a'), (2, 'b');
SELECT 'from foo_merge_tree';
SELECT * FROM foo_merge_tree;
CREATE TABLE clone_as_foo_merge_tree CLONE AS foo_merge_tree; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE IF EXISTS clone_as_foo_merge_tree;
DROP TABLE IF EXISTS foo_merge_tree;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
