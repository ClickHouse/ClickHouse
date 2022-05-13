select 'disable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=false;

select 'create table with AUTO_INCREMENT, compatibility disabled';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id int AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple(); -- {serverError SYNTAX_ERROR}

select 'enable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=true;

select 'create table with AUTO_INCREMENT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id int AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();

select 'create table with AUTO_INCREMENT, without column type';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE ignore_auto_increment;

select 'create table with several AUTO_INCREMENT';
DROP TABLE IF EXISTS ignore_auto_increment SYNC;
CREATE TABLE ignore_auto_increment (
    id int AUTO_INCREMENT, di AUTO_INCREMENT, s String AUTO_INCREMENT
) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE TABLE ignore_auto_increment;

select 'disable AUTO_INCREMENT compatibility mode';
set compatibility_ignore_auto_increment_in_create_table=false;

DROP TABLE IF EXISTS ignore_auto_increment SYNC;
