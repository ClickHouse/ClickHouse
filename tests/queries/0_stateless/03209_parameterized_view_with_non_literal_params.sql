SET allow_experimental_analyzer = 1;
select 'Test with Date parameter';

drop table if exists date_table_pv;
create table date_table_pv (id Int32, dt Date) engine = Memory();

insert into date_table_pv values(1, today());
insert into date_table_pv values(2, yesterday());
insert into date_table_pv values(3, toDate('1974-04-07'));

drop view if exists date_pv;
create view date_pv as select * from date_table_pv where dt =  {dtparam:Date};

select id from date_pv(dtparam=today());
select id from date_pv(dtparam=yesterday());
select id from date_pv(dtparam=yesterday()+1);
select id from date_pv(dtparam='1974-04-07');
select id from date_pv(dtparam=toDate('1974-04-07'));
select id from date_pv(dtparam=toString(toDate('1974-04-07')));
select id from date_pv(dtparam=toDate('1975-04-07'));
select id from date_pv(dtparam=(select dt from date_table_pv where id = 2));

select 'Test with Date32 parameter';

drop table if exists date32_table_pv;
create table date32_table_pv (id Int32, dt Date32) engine = Memory();

insert into date32_table_pv values(1, today());
insert into date32_table_pv values(2, yesterday());
insert into date32_table_pv values(3, toDate32('2199-12-31'));
insert into date32_table_pv values(4, toDate32('1950-12-25'));
insert into date32_table_pv values(5, toDate32('1900-01-01'));

drop view if exists date32_pv;
create view date32_pv as select * from date32_table_pv where dt =  {dtparam:Date32};

select id from date32_pv(dtparam=today());
select id from date32_pv(dtparam=yesterday());
select id from date32_pv(dtparam=yesterday()+1);
select id from date32_pv(dtparam='2199-12-31');
select id from date32_pv(dtparam=toDate32('1900-01-01'));
select id from date32_pv(dtparam=(select dt from date32_table_pv where id = 3));
select id from date32_pv(dtparam=(select dt from date32_table_pv where id = 4));


select 'Test with UUID parameter';
drop table if exists uuid_table_pv;
create table uuid_table_pv (id Int32, uu UUID) engine = Memory();

insert into uuid_table_pv values(1, generateUUIDv4());
insert into uuid_table_pv values(2, generateUUIDv7());
insert into uuid_table_pv values(3, toUUID('11111111-2222-3333-4444-555555555555'));
insert into uuid_table_pv select 4, serverUUID();


drop view if exists uuid_pv;
create view uuid_pv as select * from uuid_table_pv where uu =  {uuidparam:UUID};
select id from uuid_pv(uuidparam=serverUUID());
select id from uuid_pv(uuidparam=toUUID('11111111-2222-3333-4444-555555555555'));
select id from uuid_pv(uuidparam='11111111-2222-3333-4444-555555555555');
select id from uuid_pv(uuidparam=(select uu from uuid_table_pv where id = 1));
select id from uuid_pv(uuidparam=(select uu from uuid_table_pv where id = 2));
-- generateUUIDv4() is not constant foldable, hence cannot be used as parameter value
select id from uuid_pv(uuidparam=generateUUIDv4()); -- { serverError UNKNOWN_QUERY_PARAMETER }
-- But nested "select generateUUIDv4()"  works!
select id from uuid_pv(uuidparam=(select generateUUIDv4()));

select 'Test with 2 parameters';

drop view if exists date_pv2;
create view date_pv2 as select * from date_table_pv where dt = {dtparam:Date} and id = {intparam:Int32};
select id from date_pv2(dtparam=today(),intparam=1);
select id from date_pv2(dtparam=today(),intparam=length('A'));
select id from date_pv2(dtparam='1974-04-07',intparam=length('AAA'));
select id from date_pv2(dtparam=toDate('1974-04-07'),intparam=length('BBB'));

select 'Test with IPv4';

drop table if exists ipv4_table_pv;
create table ipv4_table_pv (id Int32, ipaddr IPv4) ENGINE = Memory();
insert into ipv4_table_pv values (1, '116.106.34.242');
insert into ipv4_table_pv values (2, '116.106.34.243');
insert into ipv4_table_pv values (3, '116.106.34.244');

drop view if exists ipv4_pv;
create view ipv4_pv as select * from ipv4_table_pv where ipaddr = {ipv4param:IPv4};
select id from ipv4_pv(ipv4param='116.106.34.242');
select id from ipv4_pv(ipv4param=toIPv4('116.106.34.243'));
select id from ipv4_pv(ipv4param=(select ipaddr from ipv4_table_pv where id=3));

drop view date_pv;
drop view date_pv2;
drop view date32_pv;
drop view uuid_pv;
drop view ipv4_pv;
drop table date_table_pv;
drop table date32_table_pv;
drop table uuid_table_pv;
drop table ipv4_table_pv;
