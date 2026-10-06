-- Tuple-related queries from tests/queries/0_stateless/03036_dynamic_read_subcolumns_small.sql.j2.

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET allow_experimental_dynamic_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test;

SELECT 'Memory';
CREATE TABLE test
(
    id UInt64,
    d Dynamic
)
ENGINE = Memory;

INSERT INTO test SELECT number, number FROM numbers(10);
INSERT INTO test SELECT number, 'str_' || toString(number) FROM numbers(10, 10);
INSERT INTO test SELECT number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1)) FROM numbers(20, 10);
INSERT INTO test SELECT number, NULL FROM numbers(30, 10);
INSERT INTO test SELECT number, multiIf(number % 4 == 3, 'str_' || toString(number), number % 4 == 2, NULL, number % 4 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1))) FROM numbers(40, 40);
INSERT INTO test SELECT number, [range((number % 10 + 1)::UInt64)]::Array(Array(Dynamic)) FROM numbers(10, 10);

SELECT count() FROM test WHERE not empty(d.`Tuple(a Array(Dynamic))`.a.String);
SELECT d, d.`Tuple(a UInt64, b String)`.a, d.`Array(Dynamic)`.`Variant(String, UInt64)`.UInt64, d.`Array(Variant(String, UInt64))`.UInt64 FROM test ORDER BY id, d;
SELECT d.`Array(Array(Dynamic))`.size1, d.`Array(Array(Dynamic))`.UInt64, d.`Array(Array(Dynamic))`.`Map(String, Tuple(a UInt64))`.values.a FROM test ORDER BY id, d;

DROP TABLE test;

SELECT 'MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000';
CREATE TABLE test
(
    id UInt64,
    d Dynamic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 10000000000;

INSERT INTO test SELECT number, number FROM numbers(10);
INSERT INTO test SELECT number, 'str_' || toString(number) FROM numbers(10, 10);
INSERT INTO test SELECT number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1)) FROM numbers(20, 10);
INSERT INTO test SELECT number, NULL FROM numbers(30, 10);
INSERT INTO test SELECT number, multiIf(number % 4 == 3, 'str_' || toString(number), number % 4 == 2, NULL, number % 4 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1))) FROM numbers(40, 40);
INSERT INTO test SELECT number, [range((number % 10 + 1)::UInt64)]::Array(Array(Dynamic)) FROM numbers(10, 10);

SELECT count() FROM test WHERE not empty(d.`Tuple(a Array(Dynamic))`.a.String);
SELECT d, d.`Tuple(a UInt64, b String)`.a, d.`Array(Dynamic)`.`Variant(String, UInt64)`.UInt64, d.`Array(Variant(String, UInt64))`.UInt64 FROM test ORDER BY id, d;
SELECT d.`Array(Array(Dynamic))`.size1, d.`Array(Array(Dynamic))`.UInt64, d.`Array(Array(Dynamic))`.`Map(String, Tuple(a UInt64))`.values.a FROM test ORDER BY id, d;

DROP TABLE test;

SELECT 'MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1';
CREATE TABLE test
(
    id UInt64,
    d Dynamic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO test SELECT number, number FROM numbers(10);
INSERT INTO test SELECT number, 'str_' || toString(number) FROM numbers(10, 10);
INSERT INTO test SELECT number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1)) FROM numbers(20, 10);
INSERT INTO test SELECT number, NULL FROM numbers(30, 10);
INSERT INTO test SELECT number, multiIf(number % 4 == 3, 'str_' || toString(number), number % 4 == 2, NULL, number % 4 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10 + 1))) FROM numbers(40, 40);
INSERT INTO test SELECT number, [range((number % 10 + 1)::UInt64)]::Array(Array(Dynamic)) FROM numbers(10, 10);

SELECT count() FROM test WHERE not empty(d.`Tuple(a Array(Dynamic))`.a.String);
SELECT d, d.`Tuple(a UInt64, b String)`.a, d.`Array(Dynamic)`.`Variant(String, UInt64)`.UInt64, d.`Array(Variant(String, UInt64))`.UInt64 FROM test ORDER BY id, d;
SELECT d.`Array(Array(Dynamic))`.size1, d.`Array(Array(Dynamic))`.UInt64, d.`Array(Array(Dynamic))`.`Map(String, Tuple(a UInt64))`.values.a FROM test ORDER BY id, d;

DROP TABLE test;
