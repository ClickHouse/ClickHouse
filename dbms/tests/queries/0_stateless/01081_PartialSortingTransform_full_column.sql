drop table if exists test_01081;

create table test_01081 (key Int) engine=MergeTree() order by key;
insert into test_01081 select * from system.numbers limit 10;
select 1 from remote('127.{1,2}', currentDatabase(), test_01081) lhs join system.one as rhs on rhs.dummy = 1 order by 1;

-- With multiple blocks triggers:
--
--     Code: 171. DB::Exception: Received from localhost:9000. DB::Exception: Received from 127.2:9000. DB::Exception: Block structure mismatch in  function connect between PartialSortingTransform and LazyOutputFormat stream: different columns:
--     _dummy Int Int32(size = 0), 1 UInt8 UInt8(size = 0)
--     _dummy Int Int32(size = 0), 1 UInt8 Const(size = 0, UInt8(size = 1)).
--
-- With experimental_use_processors=1 (default at the time of writing).

insert into test_01081 select * from system.numbers limit 10;
select 1 from remote('127.{1,2}', currentDatabase(), test_01081) lhs join system.one as rhs on rhs.dummy = 1 order by 1;

drop table if exists test_01081;
