drop table if exists tsv;
create table tsv(a int, b int default 7) engine File(TSV);

insert into tsv(a) select number from numbers(10000000);
select '10000000';
select count() from tsv;


insert into tsv(a) select number from numbers(10000000);
select '20000000';
select count() from tsv;


insert into tsv(a) select number from numbers(10000000);
select '30000000';
select count() from tsv;


drop table tsv;
