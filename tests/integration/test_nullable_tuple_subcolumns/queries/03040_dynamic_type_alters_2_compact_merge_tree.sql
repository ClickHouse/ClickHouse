-- Tuple-related queries from tests/queries/0_stateless/03040_dynamic_type_alters_2_compact_merge_tree.sql.

SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    x UInt64,
    y UInt64
)
ENGINE = MergeTree
ORDER BY x
SETTINGS min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 1000000000;

SELECT 'initial insert';
INSERT INTO test SELECT number, number FROM numbers(3);

SELECT 'alter add column';
ALTER TABLE test ADD COLUMN d Dynamic SETTINGS mutations_sync = 1;
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter add column 1';
INSERT INTO test SELECT number, number, number FROM numbers(3, 3);
INSERT INTO test SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
INSERT INTO test SELECT number, number, NULL FROM numbers(9, 3);
INSERT INTO test SELECT number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) FROM numbers(12, 3);
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'alter rename column 1';
ALTER TABLE test RENAME COLUMN d TO d1 SETTINGS mutations_sync = 1;
SELECT x, y, d1, d1.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert nested dynamic';
INSERT INTO test SELECT number, number, [number % 2 ? number : 'str_' || toString(number)]::Array(Dynamic) FROM numbers(15, 3);
SELECT x, y, d1, d1.`Tuple(a UInt64)`.a, d1.`Array(Dynamic)`.UInt64, d1.`Array(Dynamic)`.String, d1.`Array(Dynamic)`.Date FROM test ORDER BY x;

SELECT 'alter rename column 2';
ALTER TABLE test RENAME COLUMN d1 TO d2 SETTINGS mutations_sync = 1;
SELECT x, y, d2, d2.`Tuple(a UInt64)`.a, d2.`Array(Dynamic)`.UInt64, d2.`Array(Dynamic)`.String, d2.`Array(Dynamic)`.Date FROM test ORDER BY x;

DROP TABLE test;
