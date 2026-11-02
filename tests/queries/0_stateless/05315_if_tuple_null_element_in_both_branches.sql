-- if() over tuples where an element is NULL in both branches.

SELECT if(number = 0, (1, NULL), (number, NULL)) FROM numbers(3);
SELECT if(number % 2, (1, NULL), (2, NULL)) FROM numbers(3);
SELECT if(number = 0, (NULL, NULL), (number, number * NULL)) FROM numbers(3);
SELECT CAST(if(number = 0, (NULL, NULL), (number, number * NULL)), 'Tuple(Nullable(UInt64), Nullable(UInt64))') FROM numbers(3);
SELECT if(number = 0, ((1, NULL), 2), ((number, NULL), 3)) FROM numbers(3);
SELECT if(number = 0, toNullable((1, NULL)), (number, NULL)) FROM numbers(3);
SELECT if(toNullable(number = 0), (1, NULL), (number, NULL)) FROM numbers(3);
SELECT CASE WHEN number = 0 THEN (1, NULL) ELSE (number, NULL) END FROM numbers(3);
SELECT arrayMap(x -> if(x, (1, NULL), (2, NULL)), [0, 1]);
SELECT if(number = 0, tuple(number, materialize(NULL)), tuple(number + 1, materialize(NULL))) FROM numbers(3);
SELECT if(number % 2, (1, NULL::Nullable(UInt8)), (2, NULL::Nullable(UInt8))) FROM numbers(3);
SELECT anyRespectNullsTupleDistinct(CAST(if(number = 0, (NULL, NULL), (number, number * NULL)), 'Tuple(Nullable(UInt64), Nullable(UInt64))')) AS r, r FROM numbers(3);
