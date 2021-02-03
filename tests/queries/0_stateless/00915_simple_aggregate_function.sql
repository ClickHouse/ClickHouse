set optimize_throw_if_noop = 1;

-- basic test
drop table if exists simple;

create table simple (id UInt64,val SimpleAggregateFunction(sum,Double)) engine=AggregatingMergeTree order by id;
insert into simple select number,number from system.numbers limit 10;

select * from simple;
select * from simple final order by id;
select toTypeName(val) from simple limit 1;

-- merge
insert into simple select number,number from system.numbers limit 10;

select * from simple final order by id;

optimize table simple final;
select * from simple;

-- complex types
drop table if exists simple;

create table simple (
    id UInt64,
    nullable_str SimpleAggregateFunction(anyLast,Nullable(String)),
    low_str SimpleAggregateFunction(anyLast,LowCardinality(Nullable(String))),
    ip SimpleAggregateFunction(anyLast,IPv4),
    status SimpleAggregateFunction(groupBitOr, UInt32),
    tup SimpleAggregateFunction(sumMap, Tuple(Array(Int32), Array(Int64))),
    tup_min SimpleAggregateFunction(minMap, Tuple(Array(Int32), Array(Int64))),
    tup_max SimpleAggregateFunction(maxMap, Tuple(Array(Int32), Array(Int64))),
    arr SimpleAggregateFunction(groupArrayArray, Array(Int32)),
    uniq_arr SimpleAggregateFunction(groupUniqArrayArray, Array(Int32)),
    arg_min SimpleAggregateFunction(argMin, Tuple(Int32, Int64)),
    arg_max SimpleAggregateFunction(argMax, Tuple(Int32, Int64))
) engine=AggregatingMergeTree order by id;

insert into simple values(1,'1','1','1.1.1.1', 1, ([1,2], [1,1]), ([1,2], [1,1]), ([1,2], [1,1]), [1,2], [1,2], (1,1), (1,1));
insert into simple values(1,null,'2','2.2.2.2', 2, ([1,3], [1,1]), ([1,3], [2,2]), ([1,3], [2,2]), [2,3,4], [2,3,4], (2,2), (2,2));
-- String longer then MAX_SMALL_STRING_SIZE (actual string length is 100)
insert into simple values(10,'10','10','10.10.10.10', 4, ([2,3], [1,1]), ([2,3], [3,3]), ([2,3], [3,3]), [], [], (3,3), (3,3));
insert into simple values(10,'2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222','20','20.20.20.20', 1, ([2, 4], [1,1]), ([2, 4], [4,4]), ([2, 4], [4,4]), [], [], (4,4), (4,4));

select * from simple final order by id;
select toTypeName(nullable_str), toTypeName(low_str), toTypeName(ip), toTypeName(status),
    toTypeName(tup), toTypeName(tup_min), toTypeName(tup_max), toTypeName(arr),
    toTypeName(uniq_arr), toTypeName(arg_min), toTypeName(arg_max)
from simple limit 1;

optimize table simple final;

drop table simple;

drop table if exists with_overflow;
create table with_overflow (
    id UInt64,
    s SimpleAggregateFunction(sumWithOverflow, UInt8)
) engine AggregatingMergeTree order by id;

insert into with_overflow select 1, 1 from numbers(256);

optimize table with_overflow final;

select 'with_overflow', * from with_overflow;
drop table with_overflow;
