-- Tags: no-random-merge-tree-settings

set enable_analyzer=1;
set serialize_query_plan = 0;
set enable_parallel_replicas = 0;
set prefer_localhost_replica=1;
set optimize_aggregation_in_order=0, optimize_read_in_order=0;

select '============ #66878';

CREATE TABLE tab0 (x UInt32, y UInt32) engine = MergeTree order by x;
insert into tab0 select number, number from numbers(8192 * 123);


select * from (explain indexes=1, actions=1, distributed=1
    select * from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x = 42
);

select '============ lambdas';

--- lambdas are not supported
select * from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where arraySum(arrayMap(y -> y + 1, [x])) = 42;
select * from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where arraySum(arrayMap(y -> x + y + 2, [x])) = 42;

select '============ #69472';

select * from (explain indexes=1, actions=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x = 42
);

select * from (explain indexes=1, actions=1, distributed=1
    select * from (select x, sum(y) from remote('127.0.0.{1,2}', currentDatabase(), tab0) group by x) where x = 42
);

select '============ in / global in';

--- IN is supported
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x in (select number + 42 from numbers(1))
);

--- GLOBAL IN is replaced to temporary table

select sum(y) from (select * from remote('127.0.0.2', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.2', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);

select sum(y) from (select * from remote('127.0.0.1', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.1', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);

select sum(y) from (select * from remote('127.0.0.{2,3}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.{2,3}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);

select sum(y) from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);

select sum(y) from (select * from remote('127.0.0.{1,2,3}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1));
select * from (explain indexes=1, distributed=1
    select sum(y) from (select * from remote('127.0.0.{1,2,3}', currentDatabase(), tab0)) where x global in (select number + 42 from numbers(1))
);

select sum(x) from (select * from remote('127.0.0.{1,2,3}', currentDatabase(), tab0)) where y global in (select number + 42 from numbers(1));
select * from (explain indexes=1, distributed=1
    select sum(x) from (select * from remote('127.0.0.{1,2,3}', currentDatabase(), tab0)) where y global in (select number + 42 from numbers(1))
);

select '============ #65638';

CREATE TABLE tab1
(
    `tenant` String,
    `recordTimestamp` Int64,
    `responseBody` String,
    `colAlias` String ALIAS responseBody || 'something else',
    INDEX ngrams colAlias TYPE ngrambf_v1(3, 2097152, 3, 0) GRANULARITY 10,
)
ENGINE = MergeTree ORDER BY recordTimestamp;

INSERT INTO tab1 SELECT * FROM generateRandom('tenant String, recordTimestamp Int64, responseBody String') LIMIT 10;


select * from (explain indexes=1, distributed=1
    select * from (select * from remote('127.0.0.{1,2}', currentDatabase(), tab1)) where (tenant,recordTimestamp) IN (
    select tenant,recordTimestamp from remote('127.0.0.{1,2}', currentDatabase(), tab1) where colAlias like '%abcd%'
));

select '============ #68030';

CREATE TABLE tab2 ENGINE=ReplacingMergeTree ORDER BY n AS SELECT intDiv(number,2) as n from numbers(8192 * 123);
CREATE VIEW test_view AS SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), tab2);

select * from (explain indexes=1, actions=1, distributed=1
    SELECT * from test_view WHERE n=100
);
