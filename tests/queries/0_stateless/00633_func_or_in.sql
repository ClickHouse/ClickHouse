drop table if exists orin_test;

create table orin_test (c1 Int32) engine=Memory;
insert into orin_test values(1), (100);

select minus(c1 = 1 or c1=2 or c1 =3, c1=5) from orin_test;

drop table orin_test;
