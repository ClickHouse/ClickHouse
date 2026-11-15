SELECT arrayMap(x -> x * sum(x), range(10)); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK, 47 }
