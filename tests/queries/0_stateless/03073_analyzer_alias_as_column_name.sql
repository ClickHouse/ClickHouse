-- https://github.com/ClickHouse/ClickHouse/issues/27068
SET enable_analyzer=1;
CREATE TABLE test ( id String, create_time DateTime ) ENGINE = MergeTree ORDER BY id;

insert into test values(1,'1970-02-01 00:00:00');
insert into test values(2,'1970-02-01 00:00:00');
insert into test values(3,'1970-03-01 00:00:00');

select id,'1997-02-01' as create_time from test where test.create_time='1970-02-01 00:00:00' ORDER BY id
