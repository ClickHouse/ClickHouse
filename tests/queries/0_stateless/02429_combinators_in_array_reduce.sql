select arrayReduce('uniqStateMap', [map(1, 2)]);
select arrayReduce('uniqStateForEach', [[1], [2]]);
select arrayReduce('uniqStateForEachMapForEach', [[map(1, [2])]]);
select arrayReduceInRanges('uniqStateMap', [(1, 3), (2, 3), (3, 3)], [map(1, 'a'), map(1, 'b'), map(1, 'c'), map(1, 'd'), map(1, 'e')]);
select arrayReduceInRanges('uniqStateForEach', [(1, 3), (2, 3), (3, 3)], [['a'], ['b'], ['c'],['d'], ['e']]);
select arrayReduceInRanges('uniqStateForEachMapForEach', [(1, 3), (2, 3), (3, 3)], [[map(1, ['a'])], [map(1, ['b'])], [map(1, ['c'])], [map(1, ['d'])], [map(1, ['e'])]]);
