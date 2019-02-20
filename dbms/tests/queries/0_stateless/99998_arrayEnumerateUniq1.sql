SET max_threads = 1;

--SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]],2);                                                                    -- f(1, y,2)     =[[1,1,1],[2,3,2],[2]] -- 1 2 3 2 2 1 3

--SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2);

--SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 1);


DROP TABLE IF EXISTS test.arrays_test;
CREATE TABLE test.arrays_test (a1 Array(UInt16), a2 Array(UInt16), a3 Array(Array(UInt16)), a4 Array(Array(UInt16)) ) ENGINE = Memory;

--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([1,2,4], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,5,4],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]);
INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([21,22,24], [22,22,21], [[21,22,23,24],[22,22,21],[23]], [[21,22,25,24],[22,22,21],[23]]), ([31,32,33], [32,32,31], [[31,32,33,34],[32,32,31],[33]], [[31,32,34,34],[32,32,31],[33]]), ([41,42,43], [42,42,41], [[41,42,43,44],[42,42,41],[43]], [[41,42,44,44],[42,42,41],[43]]);

--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]), ([1,2,4], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,5],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);

--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
SELECT * FROM test.arrays_test;
select '---------GO1';
--SELECT arrayEnumerateUniqRanked(1,a1,1,a2,1) FROM test.arrays_test;
--SELECT a1,a2,'=',arrayEnumerateUniq(a1, a2) FROM test.arrays_test;
select '---------GO2';
SELECT arrayEnumerateUniqRanked(1,a3,2,a4,1) FROM test.arrays_test;
select '---------END';
--DROP TABLE test.arrays_test;
