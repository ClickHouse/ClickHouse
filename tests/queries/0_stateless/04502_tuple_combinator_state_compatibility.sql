-- The `-Tuple` state is a concatenation of per-element nested states, so two `-Tuple` states must be
-- interchangeable (common type resolution, `-Merge` acceptance) exactly when the nested states are
-- interchangeable pairwise.

SELECT 'alias spellings unify';
-- `quantileExact` and `quantilesExact` share the same state representation, so their `-Tuple` states
-- resolve to a common type and states from both spellings merge together.
SELECT quantilesExactTupleMerge(0.5)(s)
FROM
(
    SELECT quantileExactTupleState((toUInt32(number), toFloat64(number))) AS s FROM numbers(1, 100)
    UNION ALL
    SELECT quantilesExactTupleState(0.9)((toUInt32(number), toFloat64(number))) FROM numbers(101, 100)
);

SELECT 'argument-independent states unify';
-- The `count` state does not depend on the argument types, so `countTuple` states over same-arity
-- tuples with different element types resolve to a common type.
SELECT countTupleMerge(s)
FROM
(
    SELECT countTupleState((toUInt8(number), toString(number))) AS s FROM numbers(10)
    UNION ALL
    SELECT countTupleState((toUInt64(number), [toString(number)])) FROM numbers(5)
);

SELECT 'only-null placeholder ignores parameters';
-- An only-null tuple element is aggregated by a placeholder with an empty state, so states produced
-- with different finalization parameters stay interchangeable, like they are without the placeholder.
-- This exercises the -Merge acceptance check `haveSameStateRepresentation`, whose default
-- implementation compares normalized state types, across differing finalization parameters.
SELECT quantileExactTupleMerge(0.9)(s)
FROM (SELECT quantileExactTupleState(0.5)((NULL, toFloat64(number))) AS s FROM numbers(1, 101));

SELECT 'placeholder and differing parameters in a union';
SELECT quantileExactTupleMerge(0.25)(s)
FROM
(
    SELECT quantileExactTupleState(0.5)((NULL, toFloat64(number))) AS s FROM numbers(1, 50)
    UNION ALL
    SELECT quantileExactTupleState(0.9)((NULL, toFloat64(number))) FROM numbers(51, 50)
);

SELECT 'placeholder after a real element';
-- The canonical name of the normalized state comes from the first tuple element, so unification
-- must also work when the only-null element is not the first one.
SELECT DISTINCT toTypeName(s)
FROM
(
    SELECT quantileExactTupleState(0.5)((toFloat64(number), NULL)) AS s FROM numbers(10)
    UNION ALL
    SELECT quantileExactTupleState(0.9)((toFloat64(number), NULL)) FROM numbers(10)
);
SELECT quantileExactTupleMerge(0.9)(s)
FROM (SELECT quantileExactTupleState(0.5)((toFloat64(number), NULL)) AS s FROM numbers(1, 101));

SELECT 'plain only-null placeholder ignores parameters';
-- The placeholder compatibility lives at the aggregate function level, so plain functions (and any
-- other combinator) behave like `-Tuple`. An only-null aggregation collapses to a plain `NULL` at query
-- level, so build the placeholder state (a single zero byte) from its serialization.
SELECT quantileExactMerge(0.9)(CAST(unhex('00'), 'AggregateFunction(quantileExact(0.5), Nullable(Nothing))'));

SELECT 'cross-parameter merge without placeholder';
SELECT quantileExactTupleMerge(0.9)(s)
FROM (SELECT quantileExactTupleState(0.5)((toUInt32(number), toFloat64(number))) AS s FROM numbers(1, 101));

SELECT 'incompatible states are still rejected';
SELECT s FROM (SELECT sumTupleState((1, 2)) AS s UNION ALL SELECT sumTupleState((1, 2, 3))) SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
SELECT s FROM (SELECT sumTupleState((1, 2)) AS s UNION ALL SELECT countTupleState((1, 2))) SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
SELECT s FROM (SELECT sumTupleState((1, 2)) AS s UNION ALL SELECT sumTupleState((1.0, 2.0))) SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
