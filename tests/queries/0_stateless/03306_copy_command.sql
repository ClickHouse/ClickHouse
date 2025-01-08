drop table if exists test;
drop table if exists test_recreated;

create table test (x UInt32) engine=Memory();
insert into test values (42);
insert into test values (43);
COPY (SELECT * FROM test) TO file('data7.csv', 'CSV', 'x UInt32');
COPY test TO file('data8.csv', 'CSV', 'x UInt32');

drop table test;
create table test_recreated (x UInt32) engine=Memory();
COPY test_recreated FROM file('data7.csv', 'CSV', 'x UInt32');
select * from test_recreated;