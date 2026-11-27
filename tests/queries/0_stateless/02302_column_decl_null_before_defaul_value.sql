select 'create table, column +type +NULL';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column +type +NOT NULL';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column +type +NULL +DEFAULT';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column +type +NOT NULL +DEFAULT';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT NOT NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column +type +DEFAULT +NULL';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT DEFAULT 1 NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column +type +DEFAULT +NOT NULL';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT DEFAULT 1 NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column -type +NULL +DEFAULT';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column -type +NOT NULL +DEFAULT';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id NOT NULL DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column -type +DEFAULT +NULL';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id DEFAULT 1 NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'create table, column -type +DEFAULT +NOT NULL';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id DEFAULT 1 NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE null_before;

select 'alter column, NULL modifier is not allowed';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
ALTER TABLE null_before ALTER COLUMN id TYPE INT NULL; -- { clientError SYNTAX_ERROR }

select 'modify column, NULL modifier is not allowed';
DROP TABLE IF EXISTS null_before SYNC;
CREATE TABLE null_before (id INT NOT NULL) ENGINE=MergeTree() ORDER BY tuple();
ALTER TABLE null_before MODIFY COLUMN id NULL DEFAULT 1; -- { clientError SYNTAX_ERROR }

DROP TABLE IF EXISTS null_before SYNC;
