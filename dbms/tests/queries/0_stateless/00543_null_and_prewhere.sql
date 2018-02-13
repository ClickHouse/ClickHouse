CREATE TABLE test.test
(
    dt Date,
    id UInt32,
    val Nullable(UInt32)
)
ENGINE = MergeTree(dt, id, 8192);

insert into test.test (dt, id, val) values ('2017-01-01', 1, 10);
insert into test.test (dt, id, val) values ('2017-01-01', 1, null);
insert into test.test (dt, id, val) values ('2017-01-01', 1, 0);

SELECT count()
FROM test.test
WHERE val = 0;

DROP TABLE IF EXISTS test.test;
