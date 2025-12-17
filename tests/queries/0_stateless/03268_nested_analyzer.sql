SELECT nested(['a', 'b'], [1, 2], materialize([3, 4]));

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    x UInt8,
    “struct.x” DEFAULT [0],
    “struct.y” ALIAS [1],
)
ENGINE = Memory;

insert into test (x) values (0);
select * from test array join struct;
select x, struct.x, struct.y from test array join struct;

DROP TABLE test;
