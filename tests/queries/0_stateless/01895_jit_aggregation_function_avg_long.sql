SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT 'Test unsigned integer values';

DROP TABLE IF EXISTS test_table_unsigned_values;
CREATE TABLE test_table_unsigned_values
(
    id UInt64,

    value1 UInt8,
    value2 UInt16,
    value3 UInt32,
    value4 UInt64
) ENGINE=TinyLog;

INSERT INTO test_table_unsigned_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, avg(value1), avg(value2), avg(value3), avg(value4) FROM test_table_unsigned_values GROUP BY id ORDER BY id;
DROP TABLE test_table_unsigned_values;

SELECT 'Test signed integer values';

DROP TABLE IF EXISTS test_table_signed_values;
CREATE TABLE test_table_signed_values
(
    id UInt64,

    value1 Int8,
    value2 Int16,
    value3 Int32,
    value4 Int64
) ENGINE=TinyLog;

INSERT INTO test_table_signed_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, avg(value1), avg(value2), avg(value3), avg(value4) FROM test_table_signed_values GROUP BY id ORDER BY id;
DROP TABLE test_table_signed_values;

SELECT 'Test float values';

DROP TABLE IF EXISTS test_table_float_values;
CREATE TABLE test_table_float_values
(
    id UInt64,

    value1 Float32,
    value2 Float64
) ENGINE=TinyLog;

INSERT INTO test_table_float_values SELECT number % 3, number, number FROM system.numbers LIMIT 120;
SELECT id, avg(value1), avg(value2) FROM test_table_float_values GROUP BY id ORDER BY id;
DROP TABLE test_table_float_values;

SELECT 'Test nullable unsigned integer values';

DROP TABLE IF EXISTS test_table_nullable_unsigned_values;
CREATE TABLE test_table_nullable_unsigned_values
(
    id UInt64,

    value1 Nullable(UInt8),
    value2 Nullable(UInt16),
    value3 Nullable(UInt32),
    value4 Nullable(UInt64)
) ENGINE=TinyLog;

INSERT INTO test_table_nullable_unsigned_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, avg(value1), avg(value2), avg(value3), avg(value4) FROM test_table_nullable_unsigned_values GROUP BY id ORDER BY id;
DROP TABLE test_table_nullable_unsigned_values;

SELECT 'Test nullable signed integer values';

DROP TABLE IF EXISTS test_table_nullable_signed_values;
CREATE TABLE test_table_nullable_signed_values
(
    id UInt64,

    value1 Nullable(Int8),
    value2 Nullable(Int16),
    value3 Nullable(Int32),
    value4 Nullable(Int64)
) ENGINE=TinyLog;

INSERT INTO test_table_nullable_signed_values SELECT number % 3, number, number, number, number FROM system.numbers LIMIT 120;
SELECT id, avg(value1), avg(value2), avg(value3), avg(value4) FROM test_table_nullable_signed_values GROUP BY id ORDER BY id;
DROP TABLE test_table_nullable_signed_values;

SELECT 'Test nullable float values';

DROP TABLE IF EXISTS test_table_nullable_float_values;
CREATE TABLE test_table_nullable_float_values
(
    id UInt64,

    value1 Nullable(Float32),
    value2 Nullable(Float64)
) ENGINE=TinyLog;

INSERT INTO test_table_nullable_float_values SELECT number % 3, number, number FROM system.numbers LIMIT 120;
SELECT id, avg(value1), avg(value2) FROM test_table_nullable_float_values GROUP BY id ORDER BY id;
DROP TABLE test_table_nullable_float_values;

SELECT 'Test null specifics';

DROP TABLE IF EXISTS test_table_null_specifics;
CREATE TABLE test_table_null_specifics
(
    id UInt64,

    value1 Nullable(UInt64),
    value2 Nullable(UInt64),
    value3 Nullable(UInt64)
) ENGINE=TinyLog;

INSERT INTO test_table_null_specifics VALUES (0, 1, 1, NULL);
INSERT INTO test_table_null_specifics VALUES (0, 2, NULL, NULL);
INSERT INTO test_table_null_specifics VALUES (0, 3, 3, NULL);

SELECT id, avg(value1), avg(value2), avg(value3) FROM test_table_null_specifics GROUP BY id ORDER BY id;
DROP TABLE IF EXISTS test_table_null_specifics;
