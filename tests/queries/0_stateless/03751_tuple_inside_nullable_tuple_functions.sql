-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

-- Value of type Nullable(Tuple(...))
SELECT
    CAST(tuple(1, 'a') AS Nullable(Tuple(Int32, String))) AS t,
    toTypeName(t),
    isNull(t);

-- tuple() taking Nullable(Tuple(...)) as an argument
SELECT
    tuple(
        CAST((1, 'a') AS Nullable(Tuple(Int32, String)))
    ) AS outer_t,
    toTypeName(outer_t);


-- Both sides non-null Nullable(Tuple)
SELECT
    tupleConcat(
        CAST((1, 2) AS Nullable(Tuple(Int32, Int32))),
        CAST((3,) AS Nullable(Tuple(Int32)))
    ) AS res,
    toTypeName(res),
    isNull(res);

-- Left side NULL
SELECT
    tupleConcat(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((3,) AS Nullable(Tuple(Int32)))
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Both sides non-null
SELECT
    tupleDivide(
        CAST((10, 20) AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 4) AS Nullable(Tuple(Int32, Int32)))
    ) AS res,
    toTypeName(res),
    isNull(res);

-- Left side NULL
SELECT
    tupleDivide(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 4) AS Nullable(Tuple(Int32, Int32)))
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);


-- Non-null Nullable(Tuple), non-null number
SELECT
    tupleDivideByNumber(
        CAST((10, 20) AS Nullable(Tuple(Int32, Int32))),
        2
    ) AS res,
    toTypeName(res),
    isNull(res);

-- NULL tuple
SELECT
    tupleDivideByNumber(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        2
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Index access on non-null Nullable(Tuple)
SELECT
    tupleElement(
        CAST((1, 'a') AS Nullable(Tuple(Int32, String))),
        2
    ) AS v,
    toTypeName(v),
    isNull(v);

-- Index access on NULL tuple
SELECT
    tupleElement(
        CAST(NULL AS Nullable(Tuple(Int32, String))),
        2
    ) AS v_null,
    toTypeName(v_null),
    isNull(v_null);

-- Name access on Nullable(named Tuple)
SELECT
    tupleElement(
        CAST(
            tuple(1 AS id, 'a' AS name)
            AS Nullable(Tuple(id Int32, name String))
        ),
        'name'
    ) AS v_name,
    toTypeName(v_name),
    isNull(v_name);

SELECT
    t.2 AS v,
    toTypeName(v),
    isNull(v)
FROM
(
    SELECT CAST((1, 'a') AS Nullable(Tuple(Int32, String))) AS t
);

-- Both non-null
SELECT
    tupleHammingDistance(
        CAST((1, 2, 3) AS Nullable(Tuple(Int32, Int32, Int32))),
        CAST((1, 2, 4) AS Nullable(Tuple(Int32, Int32, Int32)))
    ) AS d,
    toTypeName(d),
    isNull(d);

-- One side NULL
SELECT
    tupleHammingDistance(
        CAST(NULL AS Nullable(Tuple(Int32, Int32, Int32))),
        CAST((1, 2, 4) AS Nullable(Tuple(Int32, Int32, Int32)))
    ) AS d_null,
    toTypeName(d_null),
    isNull(d_null);

-- Non-null
SELECT
    tupleIntDiv(
        CAST((10, 9) AS Nullable(Tuple(Int32, Int32))),
        CAST((3, 2) AS Nullable(Tuple(Int32, Int32)))
    ) AS res,
    toTypeName(res),
    isNull(res);

-- Left side NULL
SELECT
    tupleIntDiv(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((3, 2) AS Nullable(Tuple(Int32, Int32)))
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Non-null
SELECT
    tupleIntDivByNumber(
        CAST((10, 9) AS Nullable(Tuple(Int32, Int32))),
        3
    ) AS res,
    toTypeName(res),
    isNull(res);

-- NULL tuple
SELECT
    tupleIntDivByNumber(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        3
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Non-null tuples, some zero divisors
SELECT
    tupleIntDivOrZero(
        CAST((10, 9) AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 0) AS Nullable(Tuple(Int32, Int32)))
    ) AS res,
    toTypeName(res),
    isNull(res);

-- NULL numerator tuple
SELECT
    tupleIntDivOrZero(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 3) AS Nullable(Tuple(Int32, Int32)))
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Non-null tuple, non-zero divisor
SELECT
    tupleIntDivOrZeroByNumber(
        CAST((10, 9) AS Nullable(Tuple(Int32, Int32))),
        3
    ) AS res,
    toTypeName(res),
    isNull(res);

-- Non-null tuple, zero divisor
SELECT
    tupleIntDivOrZeroByNumber(
        CAST((10, 9) AS Nullable(Tuple(Int32, Int32))),
        0
    ) AS res_zero,
    toTypeName(res_zero),
    isNull(res_zero);

-- NULL tuple
SELECT
    tupleIntDivOrZeroByNumber(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        3
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Both non-null
SELECT
    tupleMinus(
        CAST((5, 4) AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 1) AS Nullable(Tuple(Int32, Int32)))
    ) AS diff,
    toTypeName(diff),
    isNull(diff);

-- One side NULL
SELECT
    tupleMinus(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 1) AS Nullable(Tuple(Int32, Int32)))
    ) AS diff_null,
    toTypeName(diff_null),
    isNull(diff_null);

-- Both non-null
SELECT
    tupleModulo(
        CAST((15, 10, 5) AS Nullable(Tuple(Int32, Int32, Int32))),
        CAST((5, 3, 2) AS Nullable(Tuple(Int32, Int32, Int32)))
    ) AS res,
    toTypeName(res),
    isNull(res);

-- One side NULL
SELECT
    tupleModulo(
        CAST(NULL AS Nullable(Tuple(Int32, Int32, Int32))),
        CAST((5, 3, 2) AS Nullable(Tuple(Int32, Int32, Int32)))
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);

-- Non-null
SELECT
    tupleModuloByNumber(
        CAST((15, 10, 5) AS Nullable(Tuple(Int32, Int32, Int32))),
        2
    ) AS res,
    toTypeName(res),
    isNull(res);

-- NULL tuple
SELECT
    tupleModuloByNumber(
        CAST(NULL AS Nullable(Tuple(Int32, Int32, Int32))),
        2
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);


-- Non-null
SELECT
    tupleMultiply(
        CAST((1, 2) AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 3) AS Nullable(Tuple(Int32, Int32)))
    ) AS prod,
    toTypeName(prod),
    isNull(prod);

-- One side NULL
SELECT
    tupleMultiply(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((2, 3) AS Nullable(Tuple(Int32, Int32)))
    ) AS prod_null,
    toTypeName(prod_null),
    isNull(prod_null);


-- Non-null
SELECT
    tupleMultiplyByNumber(
        CAST((1, 2) AS Nullable(Tuple(Int32, Int32))),
        -2.1
    ) AS prod,
    toTypeName(prod),
    isNull(prod);

-- NULL tuple
SELECT
    tupleMultiplyByNumber(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        -2.1
    ) AS prod_null,
    toTypeName(prod_null),
    isNull(prod_null);


-- Non-null
SELECT
    tupleNegate(
        CAST((1, -2) AS Nullable(Tuple(Int32, Int32)))
    ) AS res,
    toTypeName(res),
    isNull(res);

-- NULL tuple
SELECT
    tupleNegate(
        CAST(NULL AS Nullable(Tuple(Int32, Int32)))
    ) AS res_null,
    toTypeName(res_null),
    isNull(res_null);


-- Both non-null
SELECT
    tuplePlus(
        CAST((1, 2) AS Nullable(Tuple(Int32, Int32))),
        CAST((10, 20) AS Nullable(Tuple(Int32, Int32)))
    ) AS sum_nn,
    toTypeName(sum_nn),
    isNull(sum_nn);

-- One side NULL
SELECT
    tuplePlus(
        CAST(NULL AS Nullable(Tuple(Int32, Int32))),
        CAST((10, 20) AS Nullable(Tuple(Int32, Int32)))
    ) AS sum_null,
    toTypeName(sum_null),
    isNull(sum_null);
