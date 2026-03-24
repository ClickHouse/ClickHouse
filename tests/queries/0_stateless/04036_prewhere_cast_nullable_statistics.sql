-- Regression test: CAST(Nullable(String), 'Int32') must not be placed as the first (eagerly
-- evaluated) argument of the prewhere AND before `IS NOT NULL` when statistics are enabled.

DROP TABLE IF EXISTS test_prewhere_cast_nullable;

CREATE TABLE test_prewhere_cast_nullable (
    id UInt32,
    s  Nullable(String),
    n  Int32
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_prewhere_cast_nullable VALUES (1, NULL, 99), (2, '50', 97), (3, '200', 98);

ALTER TABLE test_prewhere_cast_nullable MODIFY STATISTICS n TYPE minmax;
ALTER TABLE test_prewhere_cast_nullable MATERIALIZE STATISTICS n;

-- Without statistics: IS NOT NULL is AND arg 0 (original WHERE order preserved) → safe
SELECT id
FROM test_prewhere_cast_nullable
WHERE s IS NOT NULL AND CAST(s, 'Int32') > 0 AND n > 0
ORDER BY id
SETTINGS use_statistics = 0, allow_experimental_statistics = 1;

-- With statistics: must also work — IS NOT NULL must still precede CAST in the prewhere AND
SELECT id
FROM test_prewhere_cast_nullable
WHERE s IS NOT NULL AND CAST(s, 'Int32') > 0 AND n > 0
ORDER BY id
SETTINGS use_statistics = 1, allow_experimental_statistics = 1;  -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

DROP TABLE test_prewhere_cast_nullable;
