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

create table XXXX (t Int64, f Float64) Engine=MergeTree order by t settings index_granularity=128, index_granularity_bytes = '10Mi';

insert into XXXX select number*60, 0 from numbers(100000);

SELECT sum(t) FROM XXXX WHERE indexHint(t = 42);

drop table if exists XXXX;

create table XXXX (t Int64, f Float64) Engine=MergeTree order by t settings index_granularity=8192, index_granularity_bytes = '10Mi';

insert into XXXX select number*60, 0 from numbers(100000);

SELECT count() FROM XXXX WHERE indexHint(t = toDateTime(0)) SETTINGS optimize_use_implicit_projections = 1;

drop table XXXX;

CREATE TABLE XXXX (p Nullable(Int64), k Decimal(76, 39)) ENGINE = MergeTree PARTITION BY toDate(p) ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO XXXX FORMAT Values ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);

SELECT count() FROM XXXX WHERE indexHint(p = 1.) SETTINGS optimize_use_implicit_projections = 1, enable_analyzer=0;
-- TODO: optimize_use_implicit_projections ignores indexHint (with analyzer) because source columns might be aliased.
SELECT count() FROM XXXX WHERE indexHint(p = 1.) SETTINGS optimize_use_implicit_projections = 1, enable_analyzer=1;

drop table XXXX;
