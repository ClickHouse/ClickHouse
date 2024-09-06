-- Tags: no-replicated-database
-- Tag no-replicated-database: Unsupported type of CREATE TABLE ... CLONE AS ... query

DROP TABLE IF EXISTS foo_memory;
DROP TABLE IF EXISTS clone_as_foo_memory;
DROP TABLE IF EXISTS foo_file;
DROP TABLE IF EXISTS clone_as_foo_file;
DROP TABLE IF EXISTS foo_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_merge_tree;
DROP TABLE IF EXISTS foo_replacing_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree;
DROP TABLE IF EXISTS foo_replicated_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree;

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

-- CLONE AS with a table of ReplicatedMergeTree engine
CREATE TABLE foo_replicated_merge_tree (x Int8, y String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_foo_replicated_merge_tree', 'r1') PRIMARY KEY x;
SHOW CREATE TABLE foo_replicated_merge_tree;
INSERT INTO foo_replicated_merge_tree VALUES (1, 'a'), (2, 'b');
SELECT * FROM foo_replicated_merge_tree;
CREATE TABLE clone_as_foo_replicated_merge_tree CLONE AS foo_replicated_merge_tree; -- { serverError SUPPORT_IS_DISABLED } 

DROP TABLE IF EXISTS foo_memory;
DROP TABLE IF EXISTS clone_as_foo_memory;
DROP TABLE IF EXISTS foo_file;
DROP TABLE IF EXISTS clone_as_foo_file;
DROP TABLE IF EXISTS foo_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_merge_tree;
DROP TABLE IF EXISTS foo_replacing_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree;
DROP TABLE IF EXISTS foo_replicated_merge_tree;
DROP TABLE IF EXISTS clone_as_foo_replicated_merge_tree;

-- CLONE AS with a Replicated database
DROP DATABASE IF EXISTS imdb_03231;

CREATE DATABASE imdb_03231 ENGINE = Replicated('/test/databases/{database}/imdb_03231', 's1', 'r1');

CREATE TABLE imdb_03231.foo_merge_tree (x Int8, y String) ENGINE=MergeTree PRIMARY KEY x;
SHOW CREATE TABLE imdb_03231.foo_merge_tree;
INSERT INTO imdb_03231.foo_merge_tree VALUES (1, 'a'), (2, 'b');
SELECT 'from imdb_03231.foo_merge_tree';
SELECT * FROM imdb_03231.foo_merge_tree;
CREATE TABLE imdb_03231.clone_as_foo_merge_tree CLONE AS imdb_03231.foo_merge_tree; -- { serverError SUPPORT_IS_DISABLED } 

DROP TABLE IF EXISTS imdb_03231.clone_as_foo_merge_tree;
DROP TABLE IF EXISTS imdb_03231.foo_merge_tree;
DROP DATABASE IF EXISTS imdb_03231;