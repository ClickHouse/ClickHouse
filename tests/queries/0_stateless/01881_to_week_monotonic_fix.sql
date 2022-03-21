drop table if exists test_tbl;

create table test_tbl (vend_nm String, ship_dt Date) engine MergeTree partition by toWeek(ship_dt) order by vend_nm;

insert into test_tbl values('1', '2020-11-11'), ('1', '2021-01-01');

select * From test_tbl where ship_dt >= toDate('2020-11-01') and ship_dt <= toDate('2021-05-05') order by ship_dt;

select * From test_tbl where ship_dt >= toDate('2020-01-01') and ship_dt <= toDate('2021-05-05') order by ship_dt;

drop table test_tbl;
