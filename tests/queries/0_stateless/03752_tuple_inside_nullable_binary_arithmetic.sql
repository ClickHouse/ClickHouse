-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

SELECT toNullable(tuple(1, 2)) * toNullable(tuple(1, 2));

SELECT (SELECT 1, 2) / (SELECT 3);

SELECT (SELECT 1, 2) % (SELECT 3);

SELECT (SELECT 1, 2) * (SELECT 3);

SELECT CAST(tuple(1, 2) AS Nullable(Tuple(Int32, Int32)))
     + CAST(tuple(3, 4) AS Nullable(Tuple(Int32, Int32)));

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     + CAST(tuple(3, 4) AS Nullable(Tuple(Int32, Int32)));

SELECT CAST(tuple(1, 2) AS Nullable(Tuple(Int32, Int32)))
     + CAST(NULL AS Nullable(Tuple(Int32, Int32)));

SELECT CAST(tuple(1, 2) AS Nullable(Tuple(Int32, Int32))) + tuple(3, 4);

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) + tuple(3, 4);

SELECT CAST(tuple(1, 2) AS Nullable(Tuple(Int32, Int32))) + 5; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) + 5; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT tuple(1, 2) + CAST(5 AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT tuple(1, 2) + CAST(NULL AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(1, 2) AS Nullable(Tuple(Int32, Int32)))
     + CAST(5 AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(1, 2) AS Nullable(Tuple(Int32, Int32)))
     + CAST(NULL AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     + CAST(5 AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     - CAST(tuple(3, 4) AS Nullable(Tuple(Int32, Int32)));

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     - CAST(tuple(3, 4) AS Nullable(Tuple(Int32, Int32)));

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     - CAST(NULL AS Nullable(Tuple(Int32, Int32)));

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32))) - tuple(3, 4);

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) - tuple(3, 4);

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32))) - 5; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) - 5; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT tuple(10, 20) - CAST(5 AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT tuple(10, 20) - CAST(NULL AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     - CAST(5 AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     - CAST(NULL AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     - CAST(5 AS Nullable(Int32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT CAST(tuple(2, 3) AS Nullable(Tuple(Int32, Int32))) * tuple(4, 5);

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) * tuple(4, 5);

SELECT CAST(tuple(2, 3) AS Nullable(Tuple(Int32, Int32))) * 5;

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) * 5;

SELECT tuple(2, 3) * CAST(5 AS Nullable(Int32));

SELECT tuple(2, 3) * CAST(NULL AS Nullable(Int32));

SELECT CAST(tuple(2, 3) AS Nullable(Tuple(Int32, Int32)))
     * CAST(5 AS Nullable(Int32));

SELECT CAST(tuple(2, 3) AS Nullable(Tuple(Int32, Int32)))
     * CAST(NULL AS Nullable(Int32));

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     * CAST(5 AS Nullable(Int32));

SELECT (SELECT 2, 3) * (SELECT 4, 5);

SELECT CAST(tuple(2, 3) AS Nullable(Tuple(Int32, Int32)))
     * (SELECT 4, 5);

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     * (SELECT 4, 5);


SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     / CAST(tuple(2, 4) AS Nullable(Tuple(Int32, Int32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     / CAST(tuple(2, 4) AS Nullable(Tuple(Int32, Int32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     / CAST(NULL AS Nullable(Tuple(Int32, Int32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32))) / tuple(2, 4); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32))) / 2;

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) / 2;

SELECT tuple(10, 20) / CAST(2 AS Nullable(Int32));

SELECT tuple(10, 20) / CAST(NULL AS Nullable(Int32));

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     / CAST(2 AS Nullable(Int32));

SELECT CAST(tuple(10, 20) AS Nullable(Tuple(Int32, Int32)))
     / CAST(NULL AS Nullable(Int32));

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     / CAST(2 AS Nullable(Int32));

SELECT CAST(tuple(10, 9) AS Nullable(Tuple(Int32, Int32)))
     % CAST(tuple(3, 2) AS Nullable(Tuple(Int32, Int32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     % CAST(tuple(3, 2) AS Nullable(Tuple(Int32, Int32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 9) AS Nullable(Tuple(Int32, Int32)))
     % CAST(NULL AS Nullable(Tuple(Int32, Int32))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 9) AS Nullable(Tuple(Int32, Int32))) % tuple(3, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT CAST(tuple(10, 9) AS Nullable(Tuple(Int32, Int32))) % 3;

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32))) % 3;

SELECT tuple(10, 9) % CAST(3 AS Nullable(Int32));

SELECT tuple(10, 9) % CAST(NULL AS Nullable(Int32));

SELECT CAST(tuple(10, 9) AS Nullable(Tuple(Int32, Int32)))
     % CAST(3 AS Nullable(Int32));

SELECT CAST(tuple(10, 9) AS Nullable(Tuple(Int32, Int32)))
     % CAST(NULL AS Nullable(Int32));

SELECT CAST(NULL AS Nullable(Tuple(Int32, Int32)))
     % CAST(3 AS Nullable(Int32));
