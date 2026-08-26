-- Regression test: when `a IS NOT NULL` (group {a}) and `CAST(a, 'Int32') * b > x`
-- (group {a, b}) are both viable PREWHERE candidates, statistics must not move the
-- multi-column group before the IS NOT NULL guard.  Without the cross-group fix the
-- CAST would execute on NULL values of `a` and throw CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN.

-- Tags: no-fasttest

DROP TABLE IF EXISTS test_prewhere_cross_group;

CREATE TABLE test_prewhere_cross_group
(
    id UInt32,
    a  Nullable(String),
    b  Int32
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_prewhere_cross_group VALUES (1, NULL, 10), (2, '5', 20), (3, '100', 5);

ALTER TABLE test_prewhere_cross_group ADD STATISTICS b TYPE minmax;
ALTER TABLE test_prewhere_cross_group MATERIALIZE STATISTICS b;

-- Without statistics: safe (IS NOT NULL preserved before CAST in prewhere).
SELECT id
FROM test_prewhere_cross_group
WHERE a IS NOT NULL AND CAST(a, 'Int32') * b > 50
ORDER BY id
SETTINGS use_statistics = 0, allow_experimental_statistics = 1;

-- With statistics: must produce the same result without throwing an exception.
-- Previously, statistics ranked `CAST(a, 'Int32') * b > 50` as more selective and moved
-- group {a, b} to prewhere before `a IS NOT NULL` (group {a}), executing the CAST on the
-- NULL row.
SELECT id
FROM test_prewhere_cross_group
WHERE a IS NOT NULL AND CAST(a, 'Int32') * b > 50
ORDER BY id
SETTINGS use_statistics = 1, allow_experimental_statistics = 1;

DROP TABLE test_prewhere_cross_group;
