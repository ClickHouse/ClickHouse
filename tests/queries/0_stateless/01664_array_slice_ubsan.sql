SELECT arraySlice(groupArray(x), -9223372036854775808, NULL) AS y FROM (SELECT '6553.5', uniqState(NULL) AS x FROM numbers(3) GROUP BY number);
