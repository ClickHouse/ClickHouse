-- Regression: LOGICAL_ERROR from `IColumn::assertTypeEquality` when a GROUP BY
-- key is a constant expression with a column-wrapping type (Const, Sparse,
-- LowCardinality wrappers in the input header that `prepareKeyColumnPtrs`
-- strips at runtime). Before the fix, `TopNDirectAggregatingTransform`'s
-- `accumulated_keys` were cloned from the input header, so they retained the
-- wrapper while runtime columns were unwrapped, breaking `insertFrom`.
-- Originally found by the AST fuzzer on PR #98607.

DROP TABLE IF EXISTS t_topn_const_key;

CREATE TABLE t_topn_const_key (k1 String, k2 String, val UInt64)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_topn_const_key SELECT
    'k1_' || toString(number % 50),
    'k2_' || toString(number % 10),
    number
FROM numbers(1000);

-- LowCardinality(UInt8) constant key (the original fuzzer trigger).
SELECT '-- toLowCardinality(0) as GROUP BY key (Mode 2 - direct hashing)';
SELECT k1, max(val) AS m
FROM t_topn_const_key
GROUP BY k1, toLowCardinality(0)
ORDER BY m DESC, k1
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Plain numeric constant key (cast to avoid positional-argument parse).
SELECT '-- numeric literal as GROUP BY key';
SELECT k1, max(val) AS m
FROM t_topn_const_key
GROUP BY k1, CAST(42 AS UInt32)
ORDER BY m DESC, k1
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- String constant key.
SELECT '-- string literal as GROUP BY key';
SELECT k1, max(val) AS m
FROM t_topn_const_key
GROUP BY k1, 'const_str'
ORDER BY m DESC, k1
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Multiple constant keys mixed with non-constant ones; forces Mode 2
-- (`TopNDirectAggregatingTransform`) on unsorted input.
SELECT '-- multiple constant keys + composite';
SELECT k1, k2, max(val) AS m
FROM t_topn_const_key
GROUP BY k1, k2, toLowCardinality(0), CAST(42 AS UInt32), 'x'
ORDER BY m DESC, k1, k2
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Reduced version of the original AST-fuzzer query (FULL OUTER JOIN of an
-- optimized vs unoptimized aggregation).
SELECT '-- original AST-fuzzer shape (FULL OUTER JOIN opt vs ref)';
SELECT count() FROM
(
    SELECT k1, k2, max(val) AS m
    FROM t_topn_const_key
    GROUP BY k1, k2, toLowCardinality(0)
    ORDER BY m DESC
    LIMIT 10
    SETTINGS optimize_topn_aggregation = 1
) AS opt
FULL OUTER JOIN
(
    SELECT k1, k2, max(val) AS m
    FROM t_topn_const_key
    GROUP BY k1, k2
    ORDER BY m DESC
    LIMIT 10
    SETTINGS optimize_topn_aggregation = 0
) AS ref USING (k1, k2, m);

DROP TABLE t_topn_const_key;
