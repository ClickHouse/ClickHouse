SELECT arrayConcat([1, 2, 3]);
SELECT number, arrayConcat(arrayMap(x -> number + x, range(3))) FROM numbers(3);
SELECT number, arrayConcat(arrayMap(x -> toString(number + x), range(2))) FROM numbers(3);
SELECT number, arrayConcat(arrayMap(x -> if(x = 0, NULL, number + x), range(3))) FROM numbers(3);
