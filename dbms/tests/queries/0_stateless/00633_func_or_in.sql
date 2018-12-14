drop table if exists test.orin_test;

create table test.orin_test (c1 Int32) engine=Memory;
insert into test.orin_test values(1), (100);

select minus(c1 = 1 or c1=2 or c1 =3, c1=5) from test.orin_test;

drop table test.orin_test;
