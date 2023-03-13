SET allow_experimental_analyzer = 1;

SELECT arrayMap(x -> x * sum(x), range(10)); -- { serverError 10 }
