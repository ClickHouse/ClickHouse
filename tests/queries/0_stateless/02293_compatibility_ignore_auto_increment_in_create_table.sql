-- Tags: no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: query is rewritten in parser
-- while adding merge tree settings

select 'disable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=false;

select 'create table failed, column +type +AUTO_INCREMENT, compatibility disabled';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id int AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple(); -- {serverError SYNTAX_ERROR}

select 'enable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=true;

select 'create table, +type +AUTO_INCREMENT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id int AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();

select 'create table, column +AUTO_INCREMENT -type';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE ignore_auto_increment;

select 'create table, several columns +/-type +AUTO_INCREMENT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id int AUTO_INCREMENT, di AUTO_INCREMENT, s String AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE ignore_auto_increment;

select 'create table, several columns with different default specifiers';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    di DEFAULT 1, id int AUTO_INCREMENT, s String EPHEMERAL
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE ignore_auto_increment;

select 'create table failed, column +type +DEFAULT +AUTO_INCREMENT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (id int DEFAULT 1 AUTO_INCREMENT) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

select 'create table failed, column -type +DEFAULT +AUTO_INCREMENT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (id int DEFAULT 1 AUTO_INCREMENT) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

select 'create table failed, column +type +AUTO_INCREMENT +DEFAULT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (id int AUTO_INCREMENT DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

select 'create table failed, column -type +AUTO_INCREMENT +DEFAULT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (id int AUTO_INCREMENT DEFAULT 1) ENGINE=MergeTree() ORDER BY tuple(); -- {clientError SYNTAX_ERROR}

DROP TABLE IF EXISTS ignore_auto_increment SYNC;
