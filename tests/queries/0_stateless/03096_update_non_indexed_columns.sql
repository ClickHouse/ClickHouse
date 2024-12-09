-- https://github.com/ClickHouse/ClickHouse/issues/39453

DROP TABLE IF EXISTS test_03096;

CREATE TABLE test_03096
(
    `a` UInt32,
    `b` UInt32,
    `c` UInt32,
    `d` UInt32 MATERIALIZED 0,
    `sum` UInt32 MATERIALIZED (a + b) + c,
    INDEX idx (c, d) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 8192;

INSERT INTO test_03096 SELECT number, number % 42, number % 123 FROM numbers(10000);

select count() from test_03096;
select count() from test_03096 where b = 0;

alter table test_03096 update b = 100 where b = 0 SETTINGS mutations_sync=2;

select latest_fail_reason == '', is_done == 1 from system.mutations where table='test_03096' and database = currentDatabase();

alter table test_03096 update b = 123 where c = 0 SETTINGS mutations_sync=2;

select latest_fail_reason == '', is_done == 1 from system.mutations where table='test_03096' and database = currentDatabase();

DROP TABLE IF EXISTS test_03096;
