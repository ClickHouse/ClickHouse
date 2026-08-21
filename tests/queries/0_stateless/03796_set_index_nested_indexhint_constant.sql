-- Test for a bug where nested indexHint with non-UInt8 constant caused a logical error
-- "ColumnUInt8 is expected as a Set index condition result"
-- The issue was that constant columns of non-UInt8 type weren't being wrapped with __bitWrapperFunc

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    k LowCardinality(Nullable(UInt16)),
    v Nullable(UInt256),
    INDEX i v TYPE set(100) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY k
SETTINGS allow_nullable_key = 1;

INSERT INTO tab SELECT number, intDiv(number, 4096) FROM numbers(1000000);

-- This query would previously crash with LOGICAL_ERROR
-- because toUInt128(0) constant wasn't wrapped with __bitWrapperFunc
SELECT DISTINCT materialize(toNullable(toUInt256(1))) FROM tab WHERE indexHint(indexHint(toUInt128(0)));
SELECT count() FROM tab WHERE indexHint(toUInt256(1));
SELECT count() FROM tab WHERE indexHint(indexHint(toInt64(1)));

DROP TABLE tab;
