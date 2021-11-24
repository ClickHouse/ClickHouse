-- Check that Buffer will be flushed before shutdown
-- (via DETACH DATABASE)

drop database if exists db_01870;
create database db_01870;

-- Right now the order for shutdown is defined and it is:
-- (prefixes are important, to define the order)
-- - a_data_01870
-- - z_buffer_01870
-- so on DETACH DATABASE the following error will be printed:
--
--     Destination table default.a_data_01870 doesn't exist. Block of data is discarded.
create table db_01870.a_data_01870 as system.numbers Engine=TinyLog();
create table db_01870.z_buffer_01870 as system.numbers Engine=Buffer(db_01870, a_data_01870, 1,
    100, 100, /* time */
    100, 100, /* rows */
    100, 1e6  /* bytes */
);
insert into db_01870.z_buffer_01870 select * from system.numbers limit 5;
select count() from db_01870.a_data_01870;
detach database db_01870;
attach database db_01870;
select count() from db_01870.a_data_01870;

drop database db_01870;
