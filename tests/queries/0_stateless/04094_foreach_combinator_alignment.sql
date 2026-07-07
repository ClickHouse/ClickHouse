-- Regression test for `-ForEach` nested-state alignment and zero-sized states.

-- Distinct (original failing combo from AST fuzzer)
SELECT sumDistinctForEach(x) FROM (SELECT [number, number % 3] AS x FROM numbers(10));
SELECT countDistinctForEach(x) FROM (SELECT [number % 5, number % 3, number % 2] AS x FROM numbers(30));

-- OrDefault + Distinct
SELECT countOrDefaultDistinctForEach([1, 2, 3]);
SELECT sumOrDefaultDistinctForEach([10, 20]);

-- OrNull
SELECT sumOrNullForEach([100, 200, 300]);

-- OrDefault / OrNull without Distinct
SELECT sumOrDefaultForEach([1, 2, 3]) FROM (SELECT * FROM numbers(3));
SELECT sumOrNullForEach([1, 2]) FROM (SELECT * FROM numbers(3));

-- OrNull + Distinct
SELECT countOrNullDistinctForEach([10, 20, 30]);

-- Zero-sized nested state from the AST fuzzer.
SELECT ignore(arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]));
