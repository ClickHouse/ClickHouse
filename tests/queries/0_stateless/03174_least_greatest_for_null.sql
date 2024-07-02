drop table if exists test1;
drop table if exists test2;

create table test1(id UInt32, x1 Nullable(Float64), x2 Nullable(Int32)) Engine=MergeTree order by id;
create table test2(id UInt32, x1 Nullable(Decimal32(5)), x2 Nullable(Decimal64(5))) Engine=MergeTree order by id;

insert into test1 values(1, 2, 3), (2, 3, NULL), (3, NULL, NULL);
insert into test2 values(1, 2, 3), (2, 3, NULL), (3, NULL, NULL);

select greatest(x1, x2), greatest(x1, x2, NULL), greatest(x1, x2, 4), greatest(x1, NULL, 4), greatest(x1, NULL, NULL), greatest(NULL, NULL, NULL) from test1;
select least(x1, x2), least(x1, x2, NULL), least(x1, x2, 4), least(x1, NULL, 4), least(x1, NULL, NULL), least(NULL, NULL, NULL) from test1;
select greatest(x1, x2), greatest(x1, x2, NULL), greatest(x1, x2, 4), greatest(x1, NULL, 4), greatest(x1, NULL, NULL), greatest(NULL, NULL, NULL) from test2;
select least(x1, x2), least(x1, x2, NULL), least(x1, x2, 4), least(x1, NULL, 4), least(x1, NULL, NULL), least(NULL, NULL, NULL) from test2;
select greatest(1,2,NULL), least(1,2,NULL);

drop table test1;
drop table test2;