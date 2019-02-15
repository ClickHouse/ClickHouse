
-- SELECT arrayEnumerateUniq([1,1,2,2,1,1], [1,2,1,2,2,2], [1,2,1,2,2,2]);
SELECT arrayEnumerateUniq(         [1,1,2,2,1,1],    [1,2,1,2,2,2]);

--SELECT arrayEnumerateUniqRanked(1, [[1,1,2,2,1,1]],2,                           [[1,2,1,2,2,2]],2); -- 1,2,3,4,1,1
SELECT arrayEnumerateUniqRanked(1, [1,1,2,2,1,1], 1, [1,2,1,2,2,2],1); -- 1,2,3,4,2,2


-- x=[1,2,1]
-- x2=[a,b,c]
-- y=[[1,2,3],[2,2,1],[3]]
-- y2=[['a','b','a'],['a','b','a'],['c']]
-- z=[[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]]


SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1);                                                      -- f(1, x,1)     =[1,1,2] -- 1 2 1
SELECT arrayEnumerateUniq([1,2,1]);
SELECT arrayEnumerateUniqRanked(1, ['a','b','c'], 1);                                                -- f(1, x2,1)    =[1,1,1] -- a b c
SELECT arrayEnumerateUniq(['a','b','c']);
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1, ['a','b','c'], 1);                                    -- f(1, x,1,x2,1)=[1,1,1] -- (1,a) (2,b) (1,c)
SELECT arrayEnumerateUniq([1,2,1], ['a','b','c']);
SELECT arrayEnumerateUniqRanked(1, [1,2,1], 1, [[1,2,3],[2,2,1],[3]], 1);                            -- f(1, x,1,y,1) =[1,1,1] -- (1,[1,2,3]) (2,[2,2,1]) (1,[3])
SELECT arrayEnumerateUniq([1,2,1], [[1,2,3],[2,2,1], [3]]);
SELECT arrayEnumerateUniqRanked(1, [['a','b','a'],['a','b','a'],['c']], 1);                          -- f(1, y2,1)    =[1,2,1] -- [a,b,a] [a,b,a] [c]
SELECT arrayEnumerateUniq([['a','b','a'],['a','b','a'],['c']]);
SELECT arrayEnumerateUniqRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]],1); -- f(1, z,1)     =[1,2,1] -- [[1,2,3],[1,2,3],[1,2,3]] [[1,2,3],[1,2,3],[1,2,3]] [[1,2]]
SELECT arrayEnumerateUniq([[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]]);

select '1,..,2';
-- подсчитываем вхождения глобально по всему значению в столбце, смотрим в глубину на два уровня,
-- ответ [[,,],[,,],[]]
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]],2);                                                                    -- f(1, y,2)     =[[1,1,1],[2,3,2],[2]] -- 1 2 3 2 2 1 3
SELECT arrayEnumerateUniqRanked(1, [['a','b','a'],['a','b','a'],['c']], 2);                                                     -- f(1, y2,2)    =[[1,1,2],[3,2,4],[1]] -- a b a a b a c
SELECT arrayEnumerateUniqRanked(1, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2);                           -- f(1, z,2)     =[[1,2,3],[4,5,6],[1]] -- [1,2,3] [1,2,3] [1,2,3] [1,2,3] [1,2,3] [1,2,3] [1,2]
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2, [['a','b','a'],['a','b','a'],['c']], 2);                           -- f(1, y,2,y2,2)=[[1,1,1],[1,2,2],[1]] -- (1,a) (2,b) (3,a) (2,a) (2,b) (1,a) (3,c)
SELECT arrayEnumerateUniqRanked(1, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2); -- f(1, y,2,z, 2)=[[1,1,1],[2,3,2],[1]] -- (1,[1,2,3]) (2,[1,2,3]) (3,[1,2,3]) (2,[1,2,3]) (2,[1,2,3]) (1,[1,2,3]) (3,[1,2])


select '2,..,2';

-- подсчитываем вхождения в отдельных массивах первого уровня, смотрим в глубину на два уровня,
-- дублируем логику arrayMap( aEU), ответ [[,,],[,,],[]]
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2);                                                                   -- f(2, y,2)=[[1,1,1],[1,2,1],[1]]  -- 1 2 3, 2 2 1, 3
SELECT arrayEnumerateUniqRanked(2, [['a','b','a'],['a','b','a'],['c']], 2);                                                     -- f(2, y2,2)=[[1,1,2],[1,1,2],[1]] -- a b a, a b a, c
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2, [['a','b','a'],['a','b','a'],['c']], 2);                           -- f(2, y,2,y2,2)=[[1,1,1],[1,1,1],[1]] -- (1,a) (2,b) (3,a), (2,a) (2,b) (1,a), (3,c)
SELECT arrayEnumerateUniqRanked(2, [[1,2,3],[2,2,1],[3]], 2, [[[1,2,3],[1,2,3],[1,2,3]],[[1,2,3],[1,2,3],[1,2,3]],[[1,2]]], 2); -- f(2, y,2,z,2)=[[1,1,1],[1,2,1],[1]] -- (1,[1,2,3]) (2,[1,2,3]) (3,[1,2,3]), (2,[1,2,3]) (2,[1,2,3]) (1,[1,2,3]), (3,[1,2])


-- SELECT max(arrayJoin(arr)) FROM (SELECT arrayEnumerateUniq(groupArray(intDiv(number, 54321)) AS nums, groupArray(toString(intDiv(number, 98765)))) AS arr FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY intHash32(number) % 100000);

--SELECT arrayEnumerateUniq([[1], [2], [34], [1]]);
--SELECT arrayEnumerateUniq([(1, 2), (3, 4), (1, 2)]);


--SELECT arrayEnumerateUniq([[1,1],[2,2]], [[1,2],[1,2]]);

--SELECT arrayEnumerateUniq(1, [1,1,2,2], [1,2,1,2]);
