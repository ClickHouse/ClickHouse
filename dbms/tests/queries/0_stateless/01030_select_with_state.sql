SELECT sumMerge(a), anyMerge(b) FROM (SELECT sum(number) AS a, any(number) AS b FROM numbers(10) WITH STATE)
