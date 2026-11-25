-- Tuple-related queries from tests/queries/0_stateless/03041_dynamic_type_check_table.sh.

SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS test;

SELECT 'MergeTree compact';
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
ALTER TABLE test ADD COLUMN d Dynamic(max_types = 2) SETTINGS mutations_sync = 1;
SELECT count(), dynamicType(d) FROM test GROUP BY dynamicType(d) ORDER BY count(), dynamicType(d);
SELECT x, y, d, d.String, d.UInt64, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter add column';
INSERT INTO test SELECT number, number, number FROM numbers(3, 3);
INSERT INTO test SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
INSERT INTO test SELECT number, number, NULL FROM numbers(9, 3);
INSERT INTO test SELECT number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) FROM numbers(12, 3);
SELECT count(), dynamicType(d) FROM test GROUP BY dynamicType(d) ORDER BY count(), dynamicType(d);
SELECT x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'check table';
CHECK TABLE test SETTINGS check_query_single_value_result = 1;

DROP TABLE test;

SELECT 'MergeTree wide';
CREATE TABLE test
(
    x UInt64,
    y UInt64
)
ENGINE = MergeTree
ORDER BY x
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

SELECT 'initial insert';
INSERT INTO test SELECT number, number FROM numbers(3);

SELECT 'alter add column';
ALTER TABLE test ADD COLUMN d Dynamic(max_types = 2) SETTINGS mutations_sync = 1;
SELECT count(), dynamicType(d) FROM test GROUP BY dynamicType(d) ORDER BY count(), dynamicType(d);
SELECT x, y, d, d.String, d.UInt64, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'insert after alter add column';
INSERT INTO test SELECT number, number, number FROM numbers(3, 3);
INSERT INTO test SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
INSERT INTO test SELECT number, number, NULL FROM numbers(9, 3);
INSERT INTO test SELECT number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) FROM numbers(12, 3);
SELECT count(), dynamicType(d) FROM test GROUP BY dynamicType(d) ORDER BY count(), dynamicType(d);
SELECT x, y, d, d.String, d.UInt64, d.Date, d.`Tuple(a UInt64)`.a FROM test ORDER BY x;

SELECT 'check table';
CHECK TABLE test SETTINGS check_query_single_value_result = 1;

DROP TABLE test;
