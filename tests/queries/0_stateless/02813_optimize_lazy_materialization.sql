SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS optimize_lazy_materialization;
CREATE TABLE optimize_lazy_materialization (a UInt64, b UInt64, c UInt64, d UInt64, n Nested(x String))
ENGINE MergeTree() PARTITION BY b ORDER BY a;
INSERT INTO optimize_lazy_materialization SELECT number, number % 2, number, number % 3, ['a', 'b', 'c'] FROM numbers(0, 100);
INSERT INTO optimize_lazy_materialization SELECT number, number % 2, number, number % 3, ['a', 'b', 'c'] FROM numbers(100, 100);

-- { echoOn }
SELECT * FROM optimize_lazy_materialization ORDER BY c LIMIT 3;
-- queries with _part_offset column in projection
SELECT a, b, c, d, _part_index, _part_offset FROM optimize_lazy_materialization ORDER BY c LIMIT 3;
SELECT _part_index, _part_offset FROM optimize_lazy_materialization ORDER BY c LIMIT 3;
-- queries with filter
SELECT * FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c LIMIT 3;
SELECT * FROM optimize_lazy_materialization PREWHERE d > 1 ORDER BY c LIMIT 3;
-- queries with function in order by
SELECT * FROM optimize_lazy_materialization WHERE d > 1 ORDER BY -c LIMIT 3;
SELECT * FROM optimize_lazy_materialization WHERE d > 1 ORDER BY -toFloat64(c) LIMIT 3;
SELECT * FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c + 1 LIMIT 3;
-- queries with function in filter
SELECT * FROM optimize_lazy_materialization WHERE d % 3 > 1 ORDER BY c LIMIT 3;
-- queries with aliases
SELECT a AS a, b AS b, c AS c, d AS d FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a AS a, b AS b, c AS c, d AS d FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a + 1 AS a, b AS b, c + 1 AS c, d + 1 AS d FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a + 1 AS a, b AS b, c + 1 AS c, d + 1 AS d FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c LIMIT 3;
-- queries with non-trivial action's chain in expression
SELECT y, z FROM (SELECT a as y, b as z FROM optimize_lazy_materialization WHERE d > 1 ORDER BY c LIMIT 3) ORDER BY y + 1;
-- queries with default value
ALTER TABLE optimize_lazy_materialization ADD COLUMN default1 UInt64;
SELECT * FROM optimize_lazy_materialization ORDER BY c LIMIT 3;
ALTER TABLE optimize_lazy_materialization ADD COLUMN default2 UInt64 ALIAS 2;
SELECT * FROM optimize_lazy_materialization ORDER BY c LIMIT 3;
ALTER TABLE optimize_lazy_materialization ADD COLUMN default3 UInt64 ALIAS a + c;
SELECT * FROM optimize_lazy_materialization ORDER BY c LIMIT 3;
-- { echoOff }
DROP TABLE IF EXISTS optimize_lazy_materialization;

-- queries with compact merge tree
CREATE TABLE optimize_lazy_materialization_with_compact_mt (a UInt64, b UInt64, c UInt64, d UInt64, n Nested(x String))
ENGINE MergeTree() PARTITION BY b ORDER BY a
settings min_rows_for_wide_part = 10000;
INSERT INTO optimize_lazy_materialization_with_compact_mt SELECT number, number % 2, number, number % 3, ['a', 'b', 'c'] FROM numbers(0, 100);
INSERT INTO optimize_lazy_materialization_with_compact_mt SELECT number, number % 2, number, number % 3, ['a', 'b', 'c'] FROM numbers(100, 100);

-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_compact_mt ORDER BY c LIMIT 3;
-- queries with _part_offset column in projection
SELECT a, b, c, d, _part_index, _part_offset FROM optimize_lazy_materialization_with_compact_mt ORDER BY c LIMIT 3;
SELECT _part_index, _part_offset FROM optimize_lazy_materialization_with_compact_mt ORDER BY c LIMIT 3;
-- queries with filter
SELECT * FROM optimize_lazy_materialization_with_compact_mt WHERE d > 1 ORDER BY c LIMIT 3;
SELECT * FROM optimize_lazy_materialization_with_compact_mt PREWHERE d > 1 ORDER BY c LIMIT 3;
-- queries with default value
ALTER TABLE optimize_lazy_materialization_with_compact_mt ADD COLUMN default1 UInt64;
SELECT * FROM optimize_lazy_materialization_with_compact_mt ORDER BY c LIMIT 3;
ALTER TABLE optimize_lazy_materialization_with_compact_mt ADD COLUMN default2 UInt64 ALIAS 2;
SELECT * FROM optimize_lazy_materialization_with_compact_mt ORDER BY c LIMIT 3;
ALTER TABLE optimize_lazy_materialization_with_compact_mt ADD COLUMN default3 UInt64 ALIAS a+c;
SELECT * FROM optimize_lazy_materialization_with_compact_mt ORDER BY c LIMIT 3;
-- { echoOff }
DROP TABLE IF EXISTS optimize_lazy_materialization_with_compact_mt;

-- queries with int data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_int_data_type;
CREATE TABLE optimize_lazy_materialization_with_int_data_type
(
    a UInt64,
    b UInt64,
    c UInt256
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_int_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number + 3
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_int_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with float data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_float_data_type;
CREATE TABLE optimize_lazy_materialization_with_float_data_type
(
    a UInt64,
    b UInt64,
    c Float64
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_float_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number + 3.1 FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_float_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with decimal data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_decimal_data_type;
CREATE TABLE optimize_lazy_materialization_with_decimal_data_type
(
    a UInt64,
    b UInt64,
    c Decimal256(1)
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_decimal_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number + 4.12
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_decimal_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with string data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_string_data_type;
CREATE TABLE optimize_lazy_materialization_with_string_data_type
(
    a UInt64,
    b UInt64,
    c String
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_string_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    repeat('a', number)
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_string_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with fixed string data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_fixed_string_data_type;
CREATE TABLE optimize_lazy_materialization_with_fixed_string_data_type
(
    a UInt64,
    b UInt64,
    c FixedString(10)
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_fixed_string_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    repeat('a', number % 10)
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_fixed_string_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with fixed date data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_date_data_type;
CREATE TABLE optimize_lazy_materialization_with_date_data_type
(
    a UInt64,
    b UInt64,
    c Date
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_date_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_date_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with fixed date32 data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_date32_data_type;
CREATE TABLE optimize_lazy_materialization_with_date32_data_type
(
    a UInt64,
    b UInt64,
    c Date32
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_date32_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_date32_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with fixed datetime data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_datetime_data_type;
CREATE TABLE optimize_lazy_materialization_with_datetime_data_type
(
    a UInt64,
    b UInt64,
    c DateTime
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_datetime_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number
FROM numbers(0, 1000);
-- { echoOn }
SELECT a, b, toUInt64(c) FROM optimize_lazy_materialization_with_datetime_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with fixed datetime64 data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_datetime64_data_type;
CREATE TABLE optimize_lazy_materialization_with_datetime64_data_type
(
    a UInt64,
    b UInt64,
    c DateTime64
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_datetime64_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number
FROM numbers(0, 1000);
-- { echoOn }
SELECT a, b, toUInt64(c) FROM optimize_lazy_materialization_with_datetime64_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with fixed enum data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_enum_data_type;
CREATE TABLE optimize_lazy_materialization_with_enum_data_type
(
    a UInt64,
    b UInt64,
    c Enum('hello', 'world')
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_enum_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number % 2 ? 'world' : 'hello'
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_enum_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with bool data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_bool_data_type;
CREATE TABLE optimize_lazy_materialization_with_bool_data_type
(
    a UInt64,
    b UInt64,
    c Bool
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_bool_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    number % 2
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_bool_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with uuid data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_uuid_data_type;
CREATE TABLE optimize_lazy_materialization_with_uuid_data_type
(
    a UInt64,
    b UInt64,
    c UUID)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_uuid_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    generateUUIDv4()
FROM numbers(0, 1000);
-- { echoOn }
SELECT a, b, length(toString(c)) FROM optimize_lazy_materialization_with_uuid_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with ipv4 data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_ipv4_data_type;
CREATE TABLE optimize_lazy_materialization_with_ipv4_data_type
(
    a UInt64,
    b UInt64,
    c IPv4
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_ipv4_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    concat('1.2.3.', toString(number % 256))
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_ipv4_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with ipv6 data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_ipv6_data_type;
CREATE TABLE optimize_lazy_materialization_with_ipv6_data_type
(
    a UInt64,
    b UInt64,
    c IPv6
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_ipv6_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    concat('1:2:3:4:5:6:7:', toString(number % 256))
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_ipv6_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with array data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_array_data_type;
CREATE TABLE optimize_lazy_materialization_with_array_data_type
(
    a UInt64,
    b UInt64,
    c Array(Tuple(field1 UInt64, field2 String))
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_array_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    [(number, toString(number + 2)), (number + 1, toString(number + 4))]
FROM numbers(0, 1000);
-- { echoOn }
SELECT a, b, c, c.size0 FROM optimize_lazy_materialization_with_array_data_type ORDER BY b LIMIT 10;
SELECT a, b, c.field2 FROM optimize_lazy_materialization_with_array_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with tuple data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_tuple_data_type;
CREATE TABLE optimize_lazy_materialization_with_tuple_data_type
(
    a UInt64,
    b UInt64,
    c Tuple(UInt64, String)
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_tuple_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    (number, toString(number * 2))
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_tuple_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with map data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_map_data_type;
CREATE TABLE optimize_lazy_materialization_with_map_data_type
(
    a UInt64,
    b UInt64,
    c Map(String, UInt64)
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_map_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    map('key1', number + 1, 'key2', number + 2)
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_map_data_type ORDER BY b LIMIT 10;
SELECT a, b, c['key1'] FROM optimize_lazy_materialization_with_map_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with variant data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_variant_data_type;
CREATE TABLE optimize_lazy_materialization_with_variant_data_type
(
    a UInt64,
    b UInt64,
    c Variant(UInt64, String, Array(UInt64))
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_variant_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    multiIf(number % 5 = 0, 666::Variant(UInt64, String, Array(UInt64)), number % 5 = 1, number::Variant(UInt64, String, Array(UInt64)), number % 5 = 2, [4, 4, 4]::Variant(UInt64, String, Array(UInt64)), NULL)
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_variant_data_type ORDER BY b LIMIT 10;
-- queries with subcolumn of variant data type
SELECT a, b, c.UInt64 FROM optimize_lazy_materialization_with_variant_data_type ORDER BY b LIMIT 10;
SELECT a, b, c.UInt64.null FROM optimize_lazy_materialization_with_variant_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with low_cardinality data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_low_cardinality_data_type;
CREATE TABLE optimize_lazy_materialization_with_low_cardinality_data_type
(
    a UInt64,
    b UInt64,
    c LowCardinality(String)
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_low_cardinality_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bbb', 'cccc')
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_low_cardinality_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with nullable data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_nullable_data_type;
CREATE TABLE optimize_lazy_materialization_with_nullable_data_type
(
    a UInt64,
    b UInt64,
    c Nullable(String)
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_nullable_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    multiIf(number % 3 = 0, 'aa', NULL)
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_nullable_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with nested data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_nested_data_type;
CREATE TABLE optimize_lazy_materialization_with_nested_data_type
(
    a UInt64,
    b UInt64,
    c Nested
    (
        id UInt32,
        order String
    )
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_nested_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    multiIf(number % 3 = 0, [1, 2, 3], [4, 5]),
    multiIf(number % 3 = 0, ['1', '2', '3'], ['4', '5'])
FROM numbers(0, 1000);
-- { echoOn }
SELECT a, b, c.id, c.order FROM optimize_lazy_materialization_with_nested_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with dynamic data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_dynamic_data_type;
CREATE TABLE optimize_lazy_materialization_with_dynamic_data_type
(
    a UInt64,
    b UInt64,
    c Dynamic
)
ENGINE MergeTree() ORDER BY a;
INSERT INTO optimize_lazy_materialization_with_dynamic_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    multiIf(number % 5 = 0, 1, number % 5 = 1, [2, 3], number % 5 = 2, '555', NULL),
FROM numbers(0, 1000);
-- { echoOn }
SELECT a, b, c FROM optimize_lazy_materialization_with_dynamic_data_type ORDER BY b LIMIT 10;
-- queries with subcolumn of dynamic data type
SELECT a, b, c.IPv4, c.String FROM optimize_lazy_materialization_with_dynamic_data_type ORDER BY b LIMIT 10;
-- { echoOff }

-- queries with sparse data type
DROP TABLE IF EXISTS optimize_lazy_materialization_with_sparse_data_type;
CREATE TABLE optimize_lazy_materialization_with_sparse_data_type
(
    a UInt64,
    b UInt64,
    c String
)
ENGINE MergeTree() ORDER BY a
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.01;;
INSERT INTO optimize_lazy_materialization_with_sparse_data_type
SELECT
    number,
    number % 2 ? 2000 : number,
    multiIf(number % 3 = 0, '', number % 3 = 1, 'aa', 'bb'),
FROM numbers(0, 1000);
-- { echoOn }
SELECT * FROM optimize_lazy_materialization_with_sparse_data_type ORDER BY b LIMIT 10;
-- { echoOff }