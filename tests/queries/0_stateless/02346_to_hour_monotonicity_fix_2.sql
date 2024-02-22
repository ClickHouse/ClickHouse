DROP TABLE IF EXISTS test;

CREATE TABLE test (stamp DateTime('UTC')) ENGINE = MergeTree PARTITION BY toDate(stamp) order by tuple() as select toDateTime('2020-01-01', 'UTC')+number*60 from numbers(1e3);

SELECT count() result FROM test WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test;
