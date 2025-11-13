-- Tags: no-parallel

drop table if exists `test_table with spaces`;

create table if not exists `test_table with spaces`
(
    `id` UInt64,
    `value` String
)
ORDER by id;

set async_insert = 1;
set wait_for_async_insert = 0;

insert into `test_table with spaces` values (1, 'a'), (2, 'b'), (3, 'c');
insert into `test_table with spaces` values (2, 'b'), (3, 'c'), (4, 'd');

system flush async insert queue `test_table with spaces`;
select '`test_table with spaces`', count() from `test_table with spaces`;

insert into `test_table with spaces` values (3, 'b'), (4, 'c'), (5, 'd');

system flush async insert queue `test_table with spaces`;
select '`test_table with spaces`', count() from `test_table with spaces`;

drop table `test_table with spaces`;


drop database if exists `this.is.a.valid.databasename`;
create database `this.is.a.valid.databasename`;

drop table if exists `this.is.a.valid.databasename`.`test_table with spaces`;
create table `this.is.a.valid.databasename`.`test_table with spaces`
(
    `id` UInt64,
    `value` String
)
ORDER by id;

insert into `this.is.a.valid.databasename`.`test_table with spaces` values (1, 'a'), (2, 'b'), (3, 'c');
insert into `this.is.a.valid.databasename`.`test_table with spaces` values (2, 'b'), (3, 'c'), (4, 'd');

system flush async insert queue `this.is.a.valid.databasename`.`test_table with spaces`;
select '`this.is.a.valid.databasename`.`test_table with spaces`', count() from `this.is.a.valid.databasename`.`test_table with spaces`;

drop table `this.is.a.valid.databasename`.`test_table with spaces`;
drop database `this.is.a.valid.databasename`;
