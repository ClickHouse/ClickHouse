-- Regression: rewrites that push a step through a UnionStep by cloning it into each branch
-- (Expression/Distinct lift-up in liftUpUnion, filter push-down in filterPushDown) must not
-- fire when a union branch type only matches the union output loosely (same aggregate state
-- representation, different type name, e.g. quantileExactTuple vs quantilesExactTuple(0.9)).
-- Previously the cloned step produced branch headers that no longer matched, tripping the
-- post-optimization "Block structure mismatch" LOGICAL_ERROR.

SET enable_analyzer = 1;

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
