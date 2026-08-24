SELECT sumForEachArray([[1],[2]]);
SELECT sumForEachArrayIf([[number],[number%2]], number < 5) from numbers(10);

SELECT groupUniqArrayMerge(x) from (select groupUniqArrayStateArray([]) as x);
SELECT groupUniqArrayArrayMerge(x) from (select groupUniqArrayArrayState([]) as x);

SELECT groupUniqArrayForEachMerge(x) from (select groupUniqArrayForEachStateArray([[1],[1],[1]]) as x);
