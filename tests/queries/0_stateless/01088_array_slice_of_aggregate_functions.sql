select arraySlice(groupArray(x), 1, 1) as y from (select uniqState(number) as x from numbers(10) group by number order by number);
