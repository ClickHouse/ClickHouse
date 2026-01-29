set enable_analyzer=1;
-- {echoOn }

SELECT nested(['a', 'b'], [1, 2], [3, 4]);
SELECT nested(['a', 'b'], [1, 2], materialize([3, 4]));
SELECT nested(['a', 'b'], materialize([1, 2]), materialize([3, 4]));

SELECT nested([['a', 'b']], [[1, 2], [3]], [[4, 5], [6]]);
SELECT nested([['a'], ['b']], [[1, 2], [3]], [[4, 5], [6]]); -- {serverError BAD_ARGUMENTS}

select x, y, z, nested(['a', 'b', 'c'], x, y, z), nested([['a', 'b', 'c']], x, y, z) from system.one array join [[[1, 2], [3]], [[4], [5, 6]]] as x, [[[7, 8], [9]], [[10], [11, 12]]] as y, [[[13, 14], [15]], [[16], [17, 18]]] as z format Pretty;
select nested([[['a', 'b', 'c']]], [[[1, 2], [3]], [[4], [5, 6]]] as x, [[[7, 8], [9]], [[10], [11, 12]]] as y, [[[13, 14], [15]], [[16], [17, 18]]] as z) format Pretty;

SELECT nested(['a', 'b'], [[1, 2], [3, 4]], [[5], [6]]);
SELECT nested([['a', 'b']], [[1, 2], [3, 4]], [[5], [6]]); -- {serverError SIZES_OF_ARRAYS_DONT_MATCH}

-- {echoOff}

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
