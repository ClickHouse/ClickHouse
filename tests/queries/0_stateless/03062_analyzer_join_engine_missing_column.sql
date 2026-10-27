-- https://github.com/ClickHouse/ClickHouse/issues/23416
SET enable_analyzer=1;
create table test (TOPIC String, PARTITION UInt64, OFFSET UInt64, ID UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03062', 'r2') ORDER BY (TOPIC, PARTITION, OFFSET);

create table test_join (TOPIC String, PARTITION UInt64, OFFSET UInt64)  ENGINE = Join(ANY, LEFT, `TOPIC`, `PARTITION`) SETTINGS join_any_take_last_row = 1;

insert into test values('abc',0,0,0);

insert into test_join values('abc',0,1);

select *, joinGet('test_join', 'OFFSET', TOPIC, PARTITION) from test;

select * from test any left join test_join using (TOPIC, PARTITION);
