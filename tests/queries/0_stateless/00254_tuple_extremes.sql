drop table if exists numbers_10;

create table numbers_10 (number UInt64) engine = MergeTree order by number;
insert into numbers_10 select number from system.numbers limit 10;

SELECT number, (number, toDate('2015-01-01') + number) FROM numbers_10 LIMIT 10 SETTINGS extremes = 1;

drop table if exists numbers_10;
