-- Regression: a UNION ALL whose branches produce aggregate-state columns that share the same
-- state representation but differ by type name (e.g. quantileExactTuple vs quantilesExactTuple(0.9))
-- must homogenize the branch types to the common union type. buildCommonHeaderForUnion picks one
-- branch's type as the common header, but the branch-conversion check (blocksHaveEqualStructure)
-- and CAST insertion (type equals()) are tolerant of that divergence, so a branch used to keep
-- emitting its own type. A step above the union that wraps the column (e.g. tuple(s)) then built a
-- column whose name embeds the branch aggregate function name, tripping the strict per-stream
-- "Block structure mismatch" check at pipeline build. This reproduces with optimizations OFF too,
-- so the base planner (addConvertingToCommonHeaderActionsIfNeeded) must force the conversion; the
-- liftUpUnion / tryPushDownFilter guards cover the sibling optimization rewrites.

SET enable_analyzer = 1;

-- Base planner variant with optimizations OFF: exercises the forced conversion before UNION,
-- independent of liftUpUnion / filterPushDown.
SELECT count() FROM
(
    SELECT tuple(s) AS ts FROM
    (
        SELECT quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 1)
        UNION ALL
        SELECT quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS s FROM numbers(101, 257)
    )
)
SETTINGS query_plan_enable_optimizations = 0;

-- Base planner variant where the divergent aggregate state is nested INSIDE the union branch
-- column itself (the -StateTuple combinator returns Tuple(AggregateFunction(...))). The union
-- column type is then Tuple(agg); the branch-conversion CAST used to take an identity wrapper
-- because the two Tuple(agg) types compare equal() while their aggregate functions differ, so the
-- source column passed through unchanged and the UnionStep-stream block-structure check aborted at
-- plan build. The CAST must rebuild the nested aggregate column with the target function.
SELECT count() IGNORE NULLS FROM
(
    SELECT tuple(s) AS ts FROM
    (
        SELECT quantileExactTupleStateTuple(tuple((toUInt32(number), toFloat64(number)))) AS s FROM numbers(100, 1)
        UNION ALL
        SELECT quantilesExactTupleStateTuple(0.9)(tuple((toUInt32(number), toFloat64(number)))) AS s FROM numbers(101, 257)
    )
)
SETTINGS query_plan_enable_optimizations = 0;

-- The exact AST-fuzzer-reduced query (DISTINCT + tuple() + WHERE above a divergent UNION ALL).
SELECT count() FROM
(
    SELECT ts FROM
    (
        SELECT DISTINCT tuple(s) AS ts, c FROM
        (
            SELECT count() AS c, quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 5) GROUP BY number % 2
            UNION ALL
            SELECT count() AS c, quantilesExactTupleStateOrNull(toDecimal64(0.9, 12))((toUInt32(number), toFloat64(number))) AS s FROM numbers(7) GROUP BY ALL
        )
    )
    WHERE 0 > c
);

-- Expression lift-up variant (the fuzzer-found crash).
SELECT count() FROM
(
    SELECT tuple(s) AS ts FROM
    (
        SELECT quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 1)
        UNION ALL
        SELECT quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS s FROM numbers(101, 257)
    )
);

-- Distinct lift-up variant.
SELECT count() FROM
(
    SELECT DISTINCT s FROM
    (
        SELECT quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 1)
        UNION ALL
        SELECT quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS s FROM numbers(101, 257)
    )
);

-- Filter -> Union push-down variant (crashes in "after optimization pushDownFilter"): the
-- UNION ALL branches carry the bare divergent aggregate state (so the union builds via the
-- state-representation-tolerant header check), a tuple() wraps it above the union, and a
-- WHERE on a sibling column stays above the union so tryPushDownFilter clones the filter into
-- each branch. The rebuilt branch headers then diverge by aggregate-state type name.
SELECT count() FROM
(
    SELECT ts FROM
    (
        SELECT tuple(s) AS ts, c FROM
        (
            SELECT count() AS c, quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 5) GROUP BY number % 2
            UNION ALL
            SELECT count() AS c, quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS s FROM numbers(200, 7) GROUP BY number % 3
        )
    )
    WHERE c > 0
);

-- The optimization must still apply for a genuine same-type union.
SELECT x + 1 AS y FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number AS x FROM numbers(3)
)
ORDER BY y;

-- Legacy interpreter path (enable_analyzer = 0): the query-tree planner is bypassed entirely, so
-- the forced conversion must also live in the legacy builders InterpreterSelectWithUnionQuery and
-- InterpreterSelectIntersectExceptQuery. Both used to gate the branch conversion on
-- blocksHaveEqualStructure + plain makeConvertingActions, leaving the divergent aggregate-state
-- type name unconverted and reaching the same "Block structure mismatch" abort at pipeline build.
SET enable_analyzer = 0;

SELECT count() FROM
(
    SELECT tuple(s) AS ts FROM
    (
        SELECT quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 5) GROUP BY number % 2
        UNION ALL
        SELECT quantilesExactTupleStateOrNull(toDecimal64(0.9, 12))((toUInt32(number), toFloat64(number))) AS s FROM numbers(7) GROUP BY ALL
    )
);

-- Same divergent-aggregate-state family through INTERSECT and EXCEPT (legacy IntersectOrExcept builder).
SELECT count() FROM
(
    SELECT tuple(s) AS ts FROM
    (
        SELECT quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(100, 5) GROUP BY number % 2
        INTERSECT
        SELECT quantilesExactTupleStateOrNull(toDecimal64(0.9, 12))((toUInt32(number), toFloat64(number))) AS s FROM numbers(7) GROUP BY ALL
    )
);

-- Legacy path must still apply the optimization for a genuine same-type union.
SELECT x + 1 AS y FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number AS x FROM numbers(3)
)
ORDER BY y;

-- Variant(...) carrier: the divergent aggregate state is wrapped in a Variant. buildCommonHeaderForUnion
-- picks branch 0's Variant type as the common header; addConvertingToCommonHeaderActionsIfNeeded forces a
-- CAST, but createVariantToVariantWrapper used to match variant members by getName() only, so the branch
-- whose nested aggregate function name differs (quantileExactTuple vs quantilesExactTuple(0.9)) was
-- rejected with CANNOT_CONVERT_TYPE even though the two Variant types are equals()-equal. The wrapper must
-- match an unmatched-by-name old member to an equals()-equal new member and physically rebuild it with the
-- target function. Covered for both analyzers and for UNION ALL / INTERSECT / EXCEPT.
SET enable_analyzer = 1;
SET allow_experimental_variant_type = 1;

SELECT count() FROM
(
    SELECT tuple(v) AS tv FROM
    (
        SELECT CAST(quantileExactTupleState((toUInt32(number), toFloat64(number))) AS Variant(AggregateFunction(quantileExactTuple, Tuple(UInt32, Float64)), UInt8)) AS v FROM numbers(100, 1)
        UNION ALL
        SELECT CAST(quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS Variant(AggregateFunction(quantilesExactTuple(0.9), Tuple(UInt32, Float64)), UInt8)) AS v FROM numbers(101, 257)
    )
)
SETTINGS query_plan_enable_optimizations = 0;

SELECT count() FROM
(
    SELECT tuple(v) AS tv FROM
    (
        SELECT CAST(quantileExactTupleState((toUInt32(number), toFloat64(number))) AS Variant(AggregateFunction(quantileExactTuple, Tuple(UInt32, Float64)), UInt8)) AS v FROM numbers(100, 1)
        INTERSECT
        SELECT CAST(quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS Variant(AggregateFunction(quantilesExactTuple(0.9), Tuple(UInt32, Float64)), UInt8)) AS v FROM numbers(101, 257)
    )
)
SETTINGS query_plan_enable_optimizations = 0;

-- Genuine Variant extension (a subset relationship, no divergent names) must still cast for free.
SELECT count() FROM
(
    SELECT CAST(1 AS Variant(UInt8)) AS v FROM numbers(2)
    UNION ALL
    SELECT CAST('x' AS Variant(UInt8, String)) AS v FROM numbers(3)
)
SETTINGS query_plan_enable_optimizations = 0;

-- Legacy interpreter path (enable_analyzer = 0) for the Variant carrier.
SET enable_analyzer = 0;

SELECT count() FROM
(
    SELECT tuple(v) AS tv FROM
    (
        SELECT CAST(quantileExactTupleState((toUInt32(number), toFloat64(number))) AS Variant(AggregateFunction(quantileExactTuple, Tuple(UInt32, Float64)), UInt8)) AS v FROM numbers(100, 1)
        UNION ALL
        SELECT CAST(quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) AS Variant(AggregateFunction(quantilesExactTuple(0.9), Tuple(UInt32, Float64)), UInt8)) AS v FROM numbers(101, 257)
    )
);
