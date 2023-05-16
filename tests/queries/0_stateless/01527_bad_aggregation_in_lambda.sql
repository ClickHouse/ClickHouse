SELECT arrayMap(x -> x * sum(x), range(10)); -- { serverError 10, 47 }
