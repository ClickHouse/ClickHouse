-- Tuple-related queries from tests/queries/0_stateless/02941_variant_type_1.sh.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    id UInt64,
    v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))
) ENGINE = Memory;

INSERT INTO test SELECT number, NULL FROM numbers(3);
INSERT INTO test SELECT number + 3, number FROM numbers(3);
INSERT INTO test SELECT number + 6, ('str_' || toString(number))::Variant(String) FROM numbers(3);
INSERT INTO test SELECT number + 9, ('lc_str_' || toString(number))::LowCardinality(String) FROM numbers(3);
INSERT INTO test SELECT number + 12, tuple(number, number + 1)::Tuple(a UInt32, b UInt32) FROM numbers(3);
INSERT INTO test SELECT number + 15, range(number + 1)::Array(UInt64) FROM numbers(3);

SELECT 'test1';
SELECT v.`Tuple(a UInt32, b UInt32)` FROM test ORDER BY id;
SELECT v.`Tuple(a UInt32, b UInt32)`.a FROM test ORDER BY id;
SELECT v.`Tuple(a UInt32, b UInt32)`.b FROM test ORDER BY id;

TRUNCATE TABLE test;

INSERT INTO test SELECT number, NULL FROM numbers(3);
INSERT INTO test SELECT number + 3, number % 2 ? NULL : number FROM numbers(3);
INSERT INTO test SELECT number + 6, number % 2 ? NULL : ('str_' || toString(number))::Variant(String) FROM numbers(3);
INSERT INTO test SELECT number + 9, number % 2 ? CAST(NULL, 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') : CAST(('lc_str_' || toString(number))::LowCardinality(String), 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') FROM numbers(3);
INSERT INTO test SELECT number + 12, number % 2 ? CAST(NULL, 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') : CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') FROM numbers(3);
INSERT INTO test SELECT number + 15, number % 2 ? CAST(NULL, 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') : CAST(range(number + 1)::Array(UInt64), 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))') FROM numbers(3);

SELECT 'test2';
SELECT v.`Tuple(a UInt32, b UInt32)` FROM test ORDER BY id;
SELECT v.`Tuple(a UInt32, b UInt32)`.a FROM test ORDER BY id;
SELECT v.`Tuple(a UInt32, b UInt32)`.b FROM test ORDER BY id;

TRUNCATE TABLE test;

INSERT INTO test WITH 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))' AS type
SELECT
    number,
    multiIf(
        number % 6 == 0, CAST(NULL, type),
        number % 6 == 1, CAST(('str_' || toString(number))::Variant(String), type),
        number % 6 == 2, CAST(number, type),
        number % 6 == 3, CAST(('lc_str_' || toString(number))::LowCardinality(String), type),
        number % 6 == 4, CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), type),
        CAST(range(number + 1)::Array(UInt64), type)
    ) AS res
FROM numbers(18);

SELECT 'test3';
SELECT v.`Tuple(a UInt32, b UInt32)` FROM test ORDER BY id;
SELECT v.`Tuple(a UInt32, b UInt32)`.a FROM test ORDER BY id;
SELECT v.`Tuple(a UInt32, b UInt32)`.b FROM test ORDER BY id;

DROP TABLE test;
