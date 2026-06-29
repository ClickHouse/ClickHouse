-- Tags: stateful
SELECT arrayFilter(x -> x != 1, arrayMap((a, b) -> a = b, GeneralInterests, arrayReduce('groupArray', GeneralInterests))) AS res FROM test.hits WHERE length(res) != 0;
