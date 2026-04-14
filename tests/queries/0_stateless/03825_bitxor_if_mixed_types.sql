-- Test for issue #70016
-- bitXor with IF expression mixing Int32 and Int64
-- This used to cause LOGICAL_ERROR: Arguments of 'bitXor' have incorrect data types

DROP TABLE IF EXISTS test_bitxor_if;

CREATE TABLE test_bitxor_if (
    c_int32 Int32,
    c_str String
) ENGINE = MergeTree() ORDER BY c_int32;

INSERT INTO test_bitxor_if VALUES (100, 'test'), (200, 'i5xv0x');

-- Simple case: bitXor with IF returning Int64 (from Int32 and Int64)
SELECT
    bitXor(
        if(c_str LIKE 'i5%v0%', c_int32, toInt64(floor(7850539625197349647))),
        c_int32
    ) as result
FROM test_bitxor_if
ORDER BY c_int32;

-- With CASE WHEN wrapping
SELECT
    CASE WHEN c_int32 > 0
    THEN bitXor(
        if(c_str LIKE 'i5%v0%', c_int32, toInt64(floor(7850539625197349647))),
        c_int32
    )
    ELSE 0
    END as result
FROM test_bitxor_if
ORDER BY c_int32;

-- Verify types
SELECT
    toTypeName(if(c_str LIKE 'i5%v0%', c_int32, toInt64(floor(7850539625197349647)))) as if_type,
    toTypeName(bitXor(
        if(c_str LIKE 'i5%v0%', c_int32, toInt64(floor(7850539625197349647))),
        c_int32
    )) as bitxor_type
FROM test_bitxor_if
LIMIT 1;

DROP TABLE test_bitxor_if;
