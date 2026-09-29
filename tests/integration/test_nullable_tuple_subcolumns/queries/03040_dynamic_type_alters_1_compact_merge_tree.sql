-- Tuple-related queries from tests/queries/0_stateless/03040_dynamic_type_alters_1_compact_merge_tree.sql.

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

SELECT 'alter add column 1';
ALTER TABLE test ADD COLUMN d Dynamic(max_types = 3) SETTINGS mutations_sync = 1;
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter add column 1';
INSERT INTO test SELECT number, number, number FROM numbers(3, 3);
INSERT INTO test SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
INSERT INTO test SELECT number, number, NULL FROM numbers(9, 3);
INSERT INTO test SELECT number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) FROM numbers(12, 3);
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'alter modify column 1';
ALTER TABLE test MODIFY COLUMN d Dynamic(max_types = 0) SETTINGS mutations_sync = 1;
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter modify column 1';
INSERT INTO test SELECT number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) FROM numbers(15, 4);
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'alter modify column 2';
ALTER TABLE test MODIFY COLUMN d Dynamic(max_types = 2) SETTINGS mutations_sync = 1;
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter modify column 2';
INSERT INTO test SELECT number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) FROM numbers(19, 4);
SELECT x, y, d, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'alter modify column 3';
ALTER TABLE test MODIFY COLUMN y Dynamic SETTINGS mutations_sync = 1;
SELECT x, y, y.`Tuple(a UInt64)`.a, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter modify column 3';
INSERT INTO test SELECT number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL), NULL FROM numbers(23, 3);
SELECT x, y, y.`Tuple(a UInt64)`.a, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

DROP TABLE test;
