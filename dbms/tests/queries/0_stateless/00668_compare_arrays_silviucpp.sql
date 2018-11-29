DROP TABLE IF EXISTS test.array;
CREATE TABLE test.array (arr Array(Nullable(Float64))) ENGINE = Memory;
INSERT INTO test.array(arr) values ([1,2]),([3,4]),([5,6]),([7,8]);

select * from test.array where arr > [12.2];
select * from test.array where arr > [null, 12.2];
select * from test.array where arr > [null, 12];

DROP TABLE test.array;
