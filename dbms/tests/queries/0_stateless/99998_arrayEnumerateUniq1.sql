SET max_threads = 1;

--SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]],2);                                                                    -- f(1, y,2)     =[[1,1,1],[2,3,2],[2]] -- 1 2 3 2 2 1 3

--SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2);

--SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 1);


DROP TABLE IF EXISTS test.arrays_test;
CREATE TABLE test.arrays_test (a1 Array(UInt16), a2 Array(UInt16), a3 Array(Array(UInt16)), a4 Array(Array(UInt16)) ) ENGINE = Memory;
        INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]), ([1,2,4], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,5],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);

--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
--INSERT INTO test.arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3],[2,2,1],[3]], [[1,2,4],[2,2,1],[3]]);
SELECT * FROM test.arrays_test;
select '---------GO1';
SELECT arrayEnumerateUniqRanked(1,a1,1,a2,1) FROM test.arrays_test;
--SELECT a1,a2,'=',arrayEnumerateUniq(a1, a2) FROM test.arrays_test;
select '---------GO2';
SELECT arrayEnumerateUniqRanked(1,a3,2,a4,2) FROM test.arrays_test;
select '---------END';
--DROP TABLE test.arrays_test;
