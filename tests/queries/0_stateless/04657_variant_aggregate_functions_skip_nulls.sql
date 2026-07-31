-- Aggregate functions that accept a Variant argument natively must still honor their documented NULL-skipping
-- contract: the rows where the Variant value is NULL are skipped, exactly as the Null combinator skips the NULL
-- values of a Nullable argument (AggregateFunctionVariantNull). Without this, `any` would return NULL from a
-- group that has non-NULL values, `groupArray` would store the NULLs its documentation promises to remove,
-- `groupConcat` would concatenate them as data, and the uniq family would count NULL as a distinct value.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_skip_nulls;
CREATE TABLE t_variant_skip_nulls (v Variant(String, UInt64), k UInt64) ENGINE = Memory;
INSERT INTO t_variant_skip_nulls VALUES (NULL, 1), (1, 2), ('a', 3), (NULL, 4), (2, 5);

-- The first row is NULL: any/anyLast/anyHeavy return the first/last non-NULL value.
SELECT 'any', any(v), anyLast(v), anyHeavy(v) FROM t_variant_skip_nulls;

-- groupArray/groupUniqArray/topK do not store the NULL rows.
SELECT 'groupArray', groupArray(v) FROM t_variant_skip_nulls;
SELECT 'groupUniqArray', arraySort(x -> toString(x), groupUniqArray(v)) FROM t_variant_skip_nulls;
SELECT 'topK', arraySort(x -> toString(x), topK(10)(v)) FROM t_variant_skip_nulls;

-- groupConcat concatenates only the non-NULL rows; an all-NULL group yields NULL (the result is Nullable, like
-- groupConcat over a Nullable argument).
SELECT 'groupConcat', groupConcat(',')(v), toTypeName(groupConcat(v)) FROM t_variant_skip_nulls;
SELECT 'groupConcat all-NULL', groupConcat(v) FROM t_variant_skip_nulls WHERE isNull(v);

-- The uniq family does not count NULL as a distinct value, matching countDistinct (see 04652).
SELECT 'uniq', uniq(v), uniqExact(v), uniqCombined(v), uniqUpTo(10)(v) FROM t_variant_skip_nulls;

-- argMin/argMax skip the rows where the returned Variant argument is NULL.
SELECT 'argMin/argMax', argMin(v, k), argMax(v, k) FROM t_variant_skip_nulls;

-- An all-NULL group produces the empty state: a Variant result reports NULL, an array result is empty.
SELECT 'all-NULL', any(v), groupArray(v), uniqExact(v) FROM t_variant_skip_nulls WHERE isNull(v);

-- Combinators compose: -If filters and the NULL rows are still skipped inside the filtered set.
SELECT 'anyIf', anyIf(v, k > 3), groupArrayIf(v, k <= 4) FROM t_variant_skip_nulls;

-- The states of the wrapped function round-trip through -State / a declared AggregateFunction type / -Merge.
SELECT 'merge', anyMerge(s), groupConcatMerge(c)
FROM
(
    SELECT anyState(v) AS s, groupConcatState(v) AS c FROM t_variant_skip_nulls
);
SELECT 'declared state', finalizeAggregation(CAST(anyState(v), 'AggregateFunction(any, Variant(String, UInt64))')) FROM t_variant_skip_nulls;

-- Merging partial states keeps the contract: a state built only from NULL rows contributes nothing.
SELECT 'partial merge', anyMerge(s) FROM
(
    SELECT anyState(v) AS s FROM t_variant_skip_nulls WHERE isNull(v)
    UNION ALL
    SELECT anyState(v) FROM t_variant_skip_nulls WHERE k = 5
);

-- GROUP BY with groups that are entirely NULL.
SELECT k % 2 AS g, any(v), groupArray(v) FROM (SELECT NULL::Variant(String, UInt64) AS v, number AS k FROM numbers(4)) GROUP BY g ORDER BY g;

-- RESPECT NULLS keeps the NULL payload rows (it is excluded from the wrapper, like it is from the Null combinator).
SELECT 'respect nulls', anyRespectNulls(v), last_value_respect_nulls(v) FROM t_variant_skip_nulls;

-- count itself skips the Variant NULLs with its dedicated implementation (see 04652), and its state stays
-- interchangeable with plain count().
SELECT 'count', count(v) FROM t_variant_skip_nulls;

DROP TABLE t_variant_skip_nulls;
