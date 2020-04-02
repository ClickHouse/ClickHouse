drop table if exists test1_00863;
drop table if exists test2_00863;
drop table if exists test3_00863;

create table test1_00863 (id UInt64, code String) engine = Memory;
create table test3_00863 (id UInt64, code String) engine = Memory;
create table test2_00863 (id UInt64, code String, test1_id UInt64, test3_id UInt64) engine = Memory;

insert into test1_00863 (id, code) select number, toString(number) FROM numbers(100000);
insert into test3_00863 (id, code) select number, toString(number) FROM numbers(100000);
insert into test2_00863 (id, code, test1_id, test3_id) select number, toString(number), number, number FROM numbers(100000);

SET max_memory_usage = 50000000;

select test2_00863.id
from test1_00863, test2_00863, test3_00863
where test1_00863.code in ('1', '2', '3')
    and test2_00863.test1_id = test1_00863.id
    and test2_00863.test3_id = test3_00863.id;

drop table test1_00863;
drop table test2_00863;
drop table test3_00863;
