drop table if exists replacing;
create table replacing(    `A` Int64,    `D` DateTime64(9, 'Europe/Moscow'),    `S` String)    ENGINE = ReplacingMergeTree(D) ORDER BY A;

insert into replacing values (1,'1970-01-01 08:25:46.300800000','a');
insert into replacing values (2,'1970-01-01 08:25:46.300800002','b');
insert into replacing values (1,'1970-01-01 08:25:46.300800003','a1');
insert into replacing values (1,'1970-01-01 08:25:46.300800002','a2');
insert into replacing values (2,'1970-01-01 08:25:46.300800004','b1');
insert into replacing values (3,'1970-01-01 08:25:46.300800005','c1');
insert into replacing values (2,'1970-01-01 08:25:46.300800005','a1');

OPTIMIZE TABLE replacing FINAL;

select * from replacing;

drop table replacing;
