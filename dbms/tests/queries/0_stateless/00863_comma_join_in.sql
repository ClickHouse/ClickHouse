drop table if exists test1;
drop table if exists test2;
drop table if exists test3;

create table test1 (id UInt64, code String) engine = Memory;
create table test3 (id UInt64, code String) engine = Memory;
create table test2 (id UInt64, code String, test1_id UInt64, test3_id UInt64) engine = Memory;

insert into test1 (id, code) select number, toString(number) FROM numbers(100000);
insert into test3 (id, code) select number, toString(number) FROM numbers(100000);
insert into test2 (id, code, test1_id, test3_id) select number, toString(number), number, number FROM numbers(100000);

select test2.id
from test1, test2, test3
where test1.code in ('1', '2', '3')
    and test2.test1_id = test1.id
    and test2.test3_id = test3.id;

drop table test1;
drop table test2;
drop table test3;
