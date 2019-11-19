drop table if exists tst;
create table tst (timestamp DateTime, val Nullable(Int8)) engine SummingMergeTree partition by toYYYYMM(timestamp) ORDER by (timestamp);
insert into tst values ('2018-02-01 00:00:00', 1), ('2018-02-02 00:00:00', 2);

select count() from tst;
select count() from tst final;

select count() from tst where timestamp is not null;
select count() from tst final where timestamp is not null;

select count() from tst where val is not null;
select count() from tst final where val is not null;

select count() from tst final where timestamp>0;
select count() from tst final prewhere timestamp > 0;
select count() from tst final where timestamp > '2017-01-01 00:00:00'

select count() from tst final where val>0;
select count() from tst final prewhere val>0;

drop table if exists tst;
create table tst (timestamp DateTime, val Int8) engine SummingMergeTree partition by toYYYYMM(timestamp) ORDER by (timestamp);
insert into tst values ('2018-02-01 00:00:00', 1), ('2018-02-02 00:00:00', 2);

select count() from tst;
select count() from tst final;

select count() from tst where timestamp is not null;
select count() from tst final where timestamp is not null;

select count() from tst where val is not null;
select count() from tst final where val is not null;

select count() from tst final where timestamp>0;
select count() from tst final prewhere timestamp > 0;
select count() from tst final where timestamp > '2017-01-01 00:00:00'

select count() from tst final where val>0;
select count() from tst final prewhere val>0;

drop table tst;
