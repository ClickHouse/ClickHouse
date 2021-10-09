-- { echo }

drop table if exists tbl;

create table tbl (p Int64, t Int64, f Float64) Engine=MergeTree partition by p order by t settings index_granularity=1;

insert into tbl select number / 4, number, 0 from numbers(16);

select * from tbl WHERE indexHint(t = 1) order by t;

select * from tbl WHERE indexHint(t in (select toInt64(number) + 2 from numbers(3))) order by t;

select * from tbl WHERE indexHint(p = 2) order by t;

select * from tbl WHERE indexHint(p in (select toInt64(number) - 2 from numbers(3))) order by t;

drop table tbl;

drop table if exists XXXX;

create table XXXX (t Int64, f Float64) Engine=MergeTree order by t settings index_granularity=128;

insert into XXXX select number*60, 0 from numbers(100000);

SELECT count() FROM XXXX WHERE indexHint(t = 42);

drop table if exists XXXX;

create table XXXX (t Int64, f Float64) Engine=MergeTree order by t settings index_granularity=8192;

insert into XXXX select number*60, 0 from numbers(100000);

SELECT count() FROM XXXX WHERE indexHint(t = toDateTime(0));

drop table XXXX;
