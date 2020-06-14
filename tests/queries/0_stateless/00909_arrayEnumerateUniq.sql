-- env SQL_FUZZY_FUNCTIONS=arrayEnumerateUniqRanked,arrayEnumerateDenseRanked SQL_FUZZY_RUNS=1000 clickhouse-test fuzzy

SELECT arrayEnumerateUniq(         [1,1,2,2,1,1],    [1,2,1,2,2,2]);
SELECT arrayEnumerateUniqRanked(1, [1,1,2,2,1,1], 1, [1,2,1,2,2,2],1);

-- x=[1,2,1]
-- x2=['a','b','c']
-- y=[[1,2,3],[2,2,1],[3]]
-- y2=[['a','b','a'],['a','b','a'],['c']]
-- z=[[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]]

SELECT 'same as arrayEnumerateUniq:';
SELECT '[1,1,2] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1);                                                      -- f(1, x,1)     =[1,1,2] -- 1 2 1
SELECT arrayEnumerateUniq(         [1,2,1]);
SELECT '[1,1,1] =';
SELECT arrayEnumerateUniqRanked(1, ['a','b','c'], 1);                                                -- f(1, x2,1)    =[1,1,1] -- a b c
SELECT arrayEnumerateUniq(         ['a','b','c']);
SELECT '[1,1,1] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1, ['a','b','c'], 1);                                    -- f(1, x,1,x2,1)=[1,1,1] -- (1,a) (2,b) (1,c)
SELECT arrayEnumerateUniq(         [1,2,1],    ['a','b','c']);
SELECT '[1,1,1] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1, [[1,2,3],[2,2,1],[3]], 1);                            -- f(1, x,1,y,1) =[1,1,1] -- (1,[1,2,3]) (2,[2,2,1]) (1,[3])
SELECT arrayEnumerateUniq(         [1,2,1],    [[1,2,3],[2,2,1],[3]]);
SELECT '[1,2,1] =';
SELECT arrayEnumerateUniqRanked(1, [['a','b','a'],['a','b','a'],['c']], 1);                          -- f(1, y2,1)    =[1,2,1] -- [a,b,a] [a,b,a] [c]
SELECT arrayEnumerateUniq(         [['a','b','a'],['a','b','a'],['c']]);
SELECT '[1,2,1] =';
SELECT arrayEnumerateUniqRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]],1); -- f(1, z,1)     =[1,2,1] -- [[1,2,3],[1,2,3],[1,2,3]] [[1,2,3],[1,2,3],[1,2,3]] [[1,2]]
SELECT arrayEnumerateUniq(         [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]]);

select '1,..,2';
-- подсчитываем вхождения глобально по всему значению в столбце, смотрим в глубину на два уровня,
-- ответ [[,,],[,,],[]]
SELECT '[[1,1,1],[2,3,2],[2]] =';
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2);                                                                   -- f(1, y,2)     =[[1,1,1],[2,3,2],[2]] -- 1 2 3 2 2 1 3
SELECT '[[1,1,2],[3,2,4],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [['a','b','a'],['a','b','a'],['c']], 2);                                                     -- f(1, y2,2)    =[[1,1,2],[3,2,4],[1]] -- a b a a b a c
SELECT '[[1,2,3],[4,5,6],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2);                           -- f(1, z,2)     =[[1,2,3],[4,5,6],[1]] -- [1,2,3] [1,2,3] [1,2,3] [1,2,3] [1,2,3] [1,2,3] [1,2]
SELECT '[[1,1,1],[1,2,2],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2, [['a','b','a'],['a','b','a'],['c']], 2);                           -- f(1, y,2,y2,2)=[[1,1,1],[1,2,2],[1]] -- (1,a) (2,b) (3,a) (2,a) (2,b) (1,a) (3,c)
SELECT '[[1,1,1],[2,3,2],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2); -- f(1, y,2,z, 2)=[[1,1,1],[2,3,2],[1]] -- (1,[1,2,3]) (2,[1,2,3]) (3,[1,2,3]) (2,[1,2,3]) (2,[1,2,3]) (1,[1,2,3]) (3,[1,2])


-- подсчитываем вхождения глобально по всему значению в столбце, смотрим в глубину на два уровня,
-- "одномерные" массивы мысленно растягиваем до "двухмерных", ответ [[,,],[,,],[]]
SELECT '[[1,1,1],[1,2,1],[2]] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1],1,[[1,2,3],[2,2,1],[3]],2); -- f(1, x,1,y,2)=[[1,1,1],[1,2,1],[2]] -- (1,1) (1,2) (1,3) (2,2) (2,2) (2,1) (1,3)
SELECT '[[1,1,1],[1,2,1],[1]] =';
SELECT arrayEnumerateUniqRanked(1, ['a','b','c'],1,[[1,2,3],[2,2,1],[3]],2); -- f(1, x2,1,y,2)=[[1,1,1],[1,2,1],[1]] -- (a,1) (a,2) (a,3) (b,2) (b,2) (b,1) (c,3)
SELECT '[[1,1,2],[1,1,2],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1],1,[['a','b','a'],['a','b','a'],['c']],2); -- f(1, x,1,y2,2)=[[1,1,2],[1,1,2],[1]] -- (1,a) (1,b) (1,a) (2,a) (2,b) (2,a) (1,c)
SELECT '[[1,1,2],[1,1,2],[1]] =';
SELECT arrayEnumerateUniqRanked(1, ['a','b','c'],1,[['a','b','a'],['a','b','a'],['c']],2); -- f(1, x2,1,y2,2)=[[1,1,2],[1,1,2],[1]] -- (a,a) (a,b) (a,a) (b,a) (b,b) (b,a) (c,c)
SELECT '[[1,2,3],[1,2,3],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1],1,[[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]],2); -- f(1, x,1,z,2)=[[1,2,3],[1,2,3],[1]] --  (1,[1,2,3]) (1,[1,2,3]) (1,[1,2,3]) (2,[1,2,3]) (2,[1,2,3]) (2,[1,2,3]) (1,[1,2])
SELECT '[[1,1,1],[1,2,1],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1],1,['a','b','c'],1,[[1,2,3],[2,2,1],[3]],2); -- f(1, x,1,x2,1,y,2)=[[1,1,1],[1,2,1],[1]] -- (1,a,1) (1,a,2) (1,a,3) (2,b,2) (2,b,2) (2,b,1) (1,c,3)
SELECT '[[1,1,1],[1,1,1],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [1,2,1],1,[[1,2,3],[2,2,1],[3]],2,[['a','b','a'],['a','b','a'],['c']],2); -- f(1, x,1,y,2,y2,2)=[[1,1,1],[1,1,1],[1]] -- (1,1,a) (1,2,b) (1,3,a) (2,2,a) (2,2,b) (2,1,a) (1,3,c)
SELECT '[[1,1,2],[1,1,2],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]],1,[['a','b','a'],['a','b','a'],['c']],2); -- f(1, y,1,y2,2)=[[1,1,2],[1,1,2],[1]] -- ([1,2,3],a) ([1,2,3],b) ([1,2,3],a) ([2,2,1],a) ([2,2,1],b) ([2,2,1],a) ([3],c)
SELECT '[[1,1,1],[2,3,2],[1]] =';
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]],2,[['a','b','a'],['a','b','a'],['c']],1); -- f(1, y,2,y2,1)=[[1,1,1],[2,3,2],[1]] -- (1,[a,b,a]) (2,[a,b,a]) (3,[a,b,a]) (2,[a,b,a]) (2,[a,b,a]) (1,[a,b,a]) (3,[c])

select '2,..,2';

-- подсчитываем вхождения в отдельных массивах первого уровня, смотрим в глубину на два уровня,
-- дублируем логику arrayMap( aEU), ответ [[,,],[,,],[]]
SELECT '[[1,1,1],[1,2,1],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2);                                                                   -- f(2, y,2)=[[1,1,1],[1,2,1],[1]]  -- 1 2 3, 2 2 1, 3
SELECT '[[1,1,2],[1,1,2],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [['a','b','a'],['a','b','a'],['c']], 2);                                                     -- f(2, y2,2)=[[1,1,2],[1,1,2],[1]] -- a b a, a b a, c
SELECT '[[1,1,1],[1,1,1],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2, [['a','b','a'],['a','b','a'],['c']], 2);                           -- f(2, y,2,y2,2)=[[1,1,1],[1,1,1],[1]] -- (1,a) (2,b) (3,a), (2,a) (2,b) (1,a), (3,c)
SELECT '[[1,1,1],[1,2,1],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2); -- f(2, y,2,z,2)=[[1,1,1],[1,2,1],[1]] -- (1,[1,2,3]) (2,[1,2,3]) (3,[1,2,3]), (2,[1,2,3]) (2,[1,2,3]) (1,[1,2,3]), (3,[1,2])

-- подсчитываем вхождения в отдельных массивах первого уровня, смотрим в глубину на два уровня,
-- "одномерные" массивы мысленно растягиваем до "двухмерных", дублируем логику arrayMap( aEU), ответ [[,,],[,,],[]]
SELECT '[[1,1,1],[1,2,1],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [1,2,1],1,[[1,2,3],[2,2,1],[3]],2); -- f(2, x,1,y,2)=[[1,1,1],[1,2,1],[1]] -- (1,1) (1,2) (1,3), (2,2) (2,2) (2,1), (1,3)
SELECT '[[1,1,2],[1,1,2],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [1,2,1],1,[['a','b','a'],['a','b','a'],['c']],2); -- f(2, x,1,y2,2)=[[1,1,2],[1,1,2],[1]] -- (1,a) (1,b) (1,a), (2,a) (2,b) (2,a), (1,c)
SELECT '[[1,2,3],[1,2,3],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [1,2,1],1,[[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]],2); -- f(2, x,1,z,2)=[[1,2,3],[1,2,3],[1]] -- (1,[1,2,3]) (1,[1,2,3]) (1,[1,2,3]), (2,[1,2,3]) (2,[1,2,3]) (2,[1,2,3]), (1,[1,2])
SELECT '[[1,2,3],[1,2,3],[1]] =';
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]],1,[[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]],2); -- f(2, y,1,z,2)=[[1,2,3],[1,2,3],[1]] -- ([1,2,3],[1,2,3]) ([1,2,3],[1,2,3]) ([1,2,3],[1,2,3]), ([2,2,1],[1,2,3]) ([2,2,1],[1,2,3]) ([2,2,1],[1,2,3]), ([3],[1,2])

select 'more:';
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]]); -- same
SELECT arrayEnumerateUniqRanked(3, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateUniqRanked(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateUniqRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateUniqRanked([[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]]); --same

select '---------Dense';
SELECT arrayEnumerateDenseRanked(1, [10,20,10,30], 1);
SELECT arrayEnumerateDense([10,20,10,30]);

SELECT arrayEnumerateDenseRanked(3, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateDenseRanked(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateDenseRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 3);
SELECT arrayEnumerateDenseRanked(2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2);
SELECT arrayEnumerateDenseRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2);
SELECT arrayEnumerateDenseRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 1);

select '---------table';

DROP TABLE IF EXISTS arrays_test;
CREATE TABLE arrays_test (a1 Array(UInt16), a2 Array(UInt16), a3 Array(Array(UInt16)), a4 Array(Array(UInt16)) ) ENGINE = Memory;

--INSERT INTO arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([1,2,4], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,5,4],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]);
INSERT INTO arrays_test VALUES ([1,2,3], [2,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([21,22,24], [22,22,21], [[21,22,23,24],[22,22,21],[23]], [[21,22,25,24],[22,22,21],[23]]), ([31,32,33], [32,32,31], [[31,32,33,34],[32,32,31],[33]], [[31,32,34,34],[32,32,31],[33]]), ([41,42,43], [42,42,41], [[41,42,43,44],[42,42,41],[43]], [[41,42,44,44],[42,42,41],[43]]);
--INSERT INTO arrays_test VALUES ([1,2,3], [1,2,1], [[1,2,3,4],[2,2,1],[3]], [[1,2,4,4],[2,2,1],[3]]), ([21,22,24], [21,22,21], [[21,22,23,24],[22,22,21],[23]], [[21,22,25,24],[22,22,21],[23]]);
INSERT INTO arrays_test VALUES ([1,1,1], [1,1,1], [[1,1,1],[1,1,1],[1]], [[1,1,1],[1,1,1],[1]]);
INSERT INTO arrays_test VALUES ([1,2,3], [4,5,6], [[7,8,9],[10,11,12],[13]], [[14,15,16],[17,18,19],[20]]);


SELECT * FROM arrays_test ORDER BY a1, a2;
select '---------GO1';
SELECT '1,a1,1', arrayEnumerateUniqRanked(1,a1,1) FROM arrays_test ORDER BY a1, a2;
SELECT '1,a2,1', arrayEnumerateUniqRanked(1,a2,1) FROM arrays_test ORDER BY a1, a2;
SELECT '1,a3,1', arrayEnumerateUniqRanked(1,a3,1) FROM arrays_test ORDER BY a1, a2;
SELECT '1,a4,1', arrayEnumerateUniqRanked(1,a4,1) FROM arrays_test ORDER BY a1, a2;
SELECT 'arrayEnumerateUniqRanked(1,a1,1,a2,1) =';
SELECT '1,a1,1,a2,1', arrayEnumerateUniqRanked(1,a1,1,a2,1) FROM arrays_test ORDER BY a1, a2;
select 'arrayEnumerateUniq(a1, a2) =';
SELECT arrayEnumerateUniq(a1, a2) FROM arrays_test ORDER BY a1, a2;
select '---------GO2';
SELECT '1,a3,1,a4,1', arrayEnumerateUniqRanked(1,a3,1,a4,1) FROM arrays_test ORDER BY a1, a2;
SELECT '1,a3,2,a4,1', arrayEnumerateUniqRanked(1,a3,2,a4,1) FROM arrays_test ORDER BY a1, a2;
SELECT '1,a3,1,a4,2', arrayEnumerateUniqRanked(1,a3,1,a4,2) FROM arrays_test ORDER BY a1, a2;
SELECT '1,a3,2,a4,2', arrayEnumerateUniqRanked(1,a3,2,a4,2) FROM arrays_test ORDER BY a1, a2;
SELECT '2,a3,2,a4,2', arrayEnumerateUniqRanked(2,a3,2,a4,2) FROM arrays_test ORDER BY a1, a2;
SELECT '2,a3,2,a4,1', arrayEnumerateUniqRanked(2,a3,2,a4,1) FROM arrays_test ORDER BY a1, a2;
SELECT '2,a3,1,a4,2', arrayEnumerateUniqRanked(2,a3,1,a4,2) FROM arrays_test ORDER BY a1, a2;
select '---------END';
DROP TABLE arrays_test;

CREATE TABLE arrays_test (a3 Array(Array(UInt8)), a4 Array(Array(UInt32)) ) ENGINE = Memory;
INSERT INTO arrays_test VALUES ([[]], [[]]), ([[1,2]], [[3,4]]), ([[5,6]], [[7,8]]), ([[]], [[]]), ([[9,10]], [[11,12]]), ([[13,14]], [[15,16]]);
SELECT 'a3,a4 1..n', arrayEnumerateUniqRanked(a3,a4) FROM arrays_test ORDER BY a3, a4;
TRUNCATE TABLE arrays_test;
INSERT INTO arrays_test VALUES ([[]], [[]]), ([[1,1]], [[1,1]]), ([[1,1]], [[1,1]]), ([[]], [[]]), ([[1,1]], [[1,1]]), ([[1,1]], [[1,1]]);
SELECT 'a3,a4 1..1', arrayEnumerateUniqRanked(a3,a4) FROM arrays_test ORDER BY a3, a4;
DROP TABLE arrays_test;



select '---------BAD';
SELECT arrayEnumerateUniqRanked(); -- { serverError 42 }
SELECT arrayEnumerateUniqRanked([]);
SELECT arrayEnumerateUniqRanked(1); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(2,[]); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(2,[],2); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(2,[],[]); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(2,[],[],3); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked([],2); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked([],2,[]); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(0,[],0); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(0,0,0); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(1,1,1); -- { serverError 36 }
SELECT arrayEnumerateDenseRanked(1, [10,20,10,30], 0); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked(1, [[7,8,9,10],[10,11,12]], 2, [[14,15,16],[17,18,19],[20],[21]], 2); -- { serverError 190 }

SELECT arrayEnumerateUniqRanked(1, [1,2], 1, ['a', 'b', 'c', 'd'],1); -- { serverError 190 }
SELECT arrayEnumerateUniqRanked(1, [1,2], 1, [14,15,16,17,18,19], 1); -- { serverError 190 }
SELECT arrayEnumerateUniqRanked(1, [14,15,16,17,18,19], 1, [1,2], 1); -- { serverError 190 }
SELECT arrayEnumerateUniqRanked(1, [1,1,1,1,1,1], 1, [1,1], 1); -- { serverError 190 }
SELECT arrayEnumerateUniqRanked(1, [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1], 1, [1,1], 1); -- { serverError 190 }
SELECT arrayEnumerateDenseRanked([], [], []);
SELECT arrayEnumerateDenseRanked([], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []);
SELECT arrayEnumerateDenseRanked([1,2], [1,2], [1,2]);
SELECT arrayEnumerateUniqRanked([1,2], [1,2], [1,2]);
SELECT arrayEnumerateUniqRanked([1,2], 3, 4, 5); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked([1,2], 1, 2); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked([1,2], 1, 3, 4, 5); -- { serverError 36 }
SELECT arrayEnumerateUniqRanked([1,2], 1, 3, [4], 5); -- { serverError 36 }
SELECT arrayEnumerateDenseRanked([[[[[[[[[[42]]]]]]]]]]);
SELECT arrayEnumerateUniqRanked('wat', [1,2]); -- { serverError 170 }
SELECT arrayEnumerateUniqRanked(1, [1,2], 'boom'); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked(['\0'], -8363126); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked(-10, ['\0'], -8363126); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked(1, ['\0'], -8363126); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked(-101, ['\0']); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked(1.1, [10,20,10,30]); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked([10,20,10,30], 0.4); -- { serverError 170 }
SELECT arrayEnumerateDenseRanked([10,20,10,30], 1.8); -- { serverError 170 }
SELECT arrayEnumerateUniqRanked(1, [], 1000000000); -- { serverError 36 }


-- skipping empty arrays
SELECT arrayEnumerateUniqRanked(2, [[3], [3]]);
SELECT arrayEnumerateUniqRanked(2, [[], [3], [3]]);
SELECT arrayEnumerateUniqRanked(2, [[], [], [], [3], [], [3]]);
SELECT arrayEnumerateUniqRanked(2, [[], [], [], [], [3], [3]]);
SELECT arrayEnumerateUniqRanked(2, [[3], [], [3]]);
SELECT arrayEnumerateUniqRanked(2, [[3], [], [], [3]]);
SELECT arrayEnumerateUniqRanked(2, [[3], [], [], [3], [3]]);


select '-- no order';
SELECT * FROM (SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [1, 2, 3, 4]] AS a UNION ALL SELECT [[3, 4, 5]] AS a ) ) ) ORDER BY a ASC;
select '-- order no arr';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[1, 2, 3, 4]] AS a UNION ALL SELECT [[3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- order two arr';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [1, 2, 3, 4]] AS a UNION ALL SELECT [[], [3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- order non empt';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[6], [1, 2, 3, 4]] AS a UNION ALL SELECT [[3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- order';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [1, 2, 3, 4]] AS a UNION ALL SELECT [[3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- ';
SELECT arrayEnumerateUniqRanked(2,[[1, 2, 3, 4], [3, 4, 5, 6]]);
SELECT arrayEnumerateUniqRanked(2, [[], [1, 2, 3, 4], [3, 4, 5, 6]]);
SELECT arrayEnumerateUniqRanked(2, [[], [1, 2, 3, 4], [], [], [3, 4, 5, 6]]);
SELECT arrayEnumerateUniqRanked(2, [[1, 2, 3, 4], [], [], [3, 4, 5, 6]]);
SELECT arrayEnumerateUniqRanked(2,[[1], [1]]);
SELECT arrayEnumerateUniqRanked(2, [[], [1], [1]]);
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [4]] AS a UNION ALL SELECT [[4]] AS a ) ORDER BY a ASC );
select '-- ';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [1, 2, 3, 4]] AS a UNION ALL SELECT [[], [3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- ';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [1, 2, 3, 4]] AS a UNION ALL SELECT [[3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- ';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [], [1, 2, 3, 4]] AS a UNION ALL SELECT [[3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- ';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [], [1, 2, 3, 4]] AS a UNION ALL SELECT [[], [], [3, 4, 5]] AS a ) ORDER BY a ASC );
select '-- ';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[], [], [1, 2, 1, 4]] AS a UNION ALL SELECT [[], [], [3, 4, 5, 4]] AS a ) ORDER BY a ASC );
select '-- ';


DROP TABLE IF EXISTS arrays_test;
CREATE TABLE arrays_test (a1 Array(UInt8), a2 Array(UInt32) ) ENGINE = Memory;
INSERT INTO arrays_test VALUES ([], []),([10], [11]), ([], []), ([12], [13]);
SELECT 'a1,a2 n', arrayEnumerateUniqRanked(a1,a2) FROM arrays_test ORDER BY a1, a2;

TRUNCATE TABLE arrays_test;
INSERT INTO arrays_test VALUES ([], []),([1], [1]), ([], []), ([1], [1]);
SELECT 'a1,a2 1', arrayEnumerateUniqRanked(a1,a2) FROM arrays_test ORDER BY a1, a2;

TRUNCATE TABLE arrays_test;
INSERT INTO arrays_test VALUES ([], []), ([1,2], [3,4]), ([5,6], [7,8]), ([], []), ([9,10], [11,12]), ([13,14], [15,16]);
SELECT 'a1,a2 n2', arrayEnumerateUniqRanked(a1,a2) FROM arrays_test ORDER BY a1, a2;

TRUNCATE TABLE arrays_test;
INSERT INTO arrays_test VALUES ([], []), ([1,1], [1,1]), ([1,1], [1,1]), ([], []), ([1,1], [1,1]), ([1,1], [1,1]);
SELECT 'a1,a2 12', arrayEnumerateUniqRanked(a1,a2) FROM arrays_test ORDER BY a1, a2;

DROP TABLE arrays_test;



DROP TABLE IF EXISTS arr_tests_visits;

CREATE TABLE arr_tests_visits
(
    CounterID        UInt32,
    StartDate        Date,
    Sign             Int8,
    VisitID          UInt64,
    UserID           UInt64,
    VisitVersion     UInt16,
    `Test.BannerID` Array(UInt64),
    `Test.Load`     Array(UInt8),
    `Test.PuidKey`  Array(Array(UInt8)),
    `Test.PuidVal`  Array(Array(UInt32))
) ENGINE = MergeTree() PARTITION BY toMonday(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192;

truncate table arr_tests_visits;
insert into arr_tests_visits (CounterID, StartDate, Sign, VisitID, UserID, VisitVersion, `Test.BannerID`, `Test.Load`, `Test.PuidKey`, `Test.PuidVal`)
values (1, toDate('2019-06-06'), 1, 1, 1, 1, [1], [1], [[]], [[]]),       (1, toDate('2019-06-06'), -1, 1, 1, 1, [1], [1], [[]], [[]]),       (1, toDate('2019-06-06'), 1, 1, 1, 2, [1,2], [1,1], [[],[1,2,3,4]], [[],[1001, 1002, 1003, 1004]]),       (1, toDate('2019-06-06'), 1, 2, 1, 1, [3], [1], [[3,4,5]], [[2001, 2002, 2003]]),       (1, toDate('2019-06-06'), 1, 3, 2, 1, [4, 5], [1, 0], [[5,6],[]], [[3001, 3002],[]]),       (1, toDate('2019-06-06'), 1, 4, 2, 1, [5, 5, 6], [1, 0, 0], [[1,2], [1, 2], [3]], [[1001, 1002],[1002, 1003], [2001]]);

select CounterID, StartDate, Sign, VisitID, UserID, VisitVersion, BannerID, Load, PuidKeyArr, PuidValArr, arrayEnumerateUniqRanked(PuidKeyArr, PuidValArr) as uniqTestPuid
    from arr_tests_visits
    array join
         Test.BannerID as BannerID,
         Test.Load as Load,
         Test.PuidKey as PuidKeyArr,
         Test.PuidVal as PuidValArr;

select '--';

SELECT
    CounterID,
    StartDate,
    Sign,
    VisitID,
    UserID,
    VisitVersion,
    BannerID,
    Load,
    PuidKeyArr,
    PuidValArr,
    arrayEnumerateUniqRanked(PuidKeyArr, PuidValArr) AS uniqTestPuid
FROM arr_tests_visits
ARRAY JOIN
    Test.BannerID AS BannerID,
    Test.Load AS Load,
    Test.PuidKey AS PuidKeyArr,
    Test.PuidVal AS PuidValArr;

DROP TABLE arr_tests_visits;


select '-- empty';
SELECT arrayEnumerateUniqRanked([['a'], [], ['a']]);
SELECT arrayEnumerateUniqRanked([[1], [], [1]]);
SELECT arrayEnumerateUniqRanked([[1], [], [1], [], [1], [], [1], [], [1], [], [1], [], [1], [], [1], [], [1]]);
SELECT arrayEnumerateUniqRanked([[], [1], [], [1], [], [1], [], [1], [], [1], [], [1], [], [1], [], [1]]);
SELECT arrayEnumerateUniqRanked([[1], [1], [], [1]]);

select '-- empty corner';
SELECT a, arrayEnumerateUniqRanked(a) FROM ( SELECT * FROM ( SELECT [[],[1],[]] AS a UNION ALL SELECT [[1]] AS a ) ORDER BY a ASC );
