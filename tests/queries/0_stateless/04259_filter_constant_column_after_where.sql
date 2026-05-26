DROP TABLE IF EXISTS 04259_filter_constant_column_after_where;

CREATE TABLE 04259_filter_constant_column_after_where
(
    x String,
    y UInt64,
    z UInt64,
    n Nullable(UInt64),
    lc LowCardinality(String)
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where VALUES
    ('hello', 1, 2, 1, 'hello'),
    ('world', 2, 2, NULL, 'world'),
    ('hello', 3, 2, 3, 'hello'),
    ('other', 4, 4, 4, 'other');

SELECT dumpColumnStructure(x), count(), sum(y)
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello'
GROUP BY ALL
ORDER BY ALL
SETTINGS query_plan_merge_filters = 0;

SELECT dumpColumnStructure(x), count(), sum(y)
FROM 04259_filter_constant_column_after_where
WHERE 'world' = x AND y >= 0
GROUP BY ALL
ORDER BY ALL
SETTINGS query_plan_merge_filters = 1;

SET query_plan_merge_filters = 0, query_plan_optimize_lazy_materialization = 0;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE y = 3
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), dumpColumnStructure(y), dumpColumnStructure(z), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello' AND y = 1 AND z = 2
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), dumpColumnStructure(y), dumpColumnStructure(z), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello' AND y = 1 AND x = 'hello'
GROUP BY ALL
ORDER BY ALL;

WITH x AS x_alias
SELECT dumpColumnStructure(x_alias), count()
FROM 04259_filter_constant_column_after_where
WHERE x_alias = 'world'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), count()
FROM
(
    SELECT 'always_true' AS x, number
    FROM numbers(3)
)
WHERE x = 'always_true'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE y = 1 OR y = 3
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE NOT (y = 1)
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE y = 1::UInt8
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(y), count()
FROM 04259_filter_constant_column_after_where
WHERE toString(y) = '1'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(n), count()
FROM 04259_filter_constant_column_after_where
WHERE n = 1
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(lc), count()
FROM 04259_filter_constant_column_after_where
WHERE lc = 'hello'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(z), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello'
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(x), count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'hello' AND y = 1
GROUP BY ALL
ORDER BY ALL;

SELECT count()
FROM 04259_filter_constant_column_after_where
WHERE x = 'missing';

SELECT reinterpretAsUInt64(f)
FROM
(
    SELECT arrayJoin([reinterpretAsFloat64(toUInt64(0)), reinterpretAsFloat64(toUInt64(9223372036854775808))]) AS f
)
WHERE f = 0.0
ORDER BY reinterpretAsUInt64(f);

SELECT reinterpretAsUInt64(tupleElement(t, 1))
FROM
(
    SELECT tuple(arrayJoin([reinterpretAsFloat64(toUInt64(0)), reinterpretAsFloat64(toUInt64(9223372036854775808))])) AS t
)
WHERE t = tuple(0.0)
ORDER BY reinterpretAsUInt64(tupleElement(t, 1));

DROP TABLE IF EXISTS 04259_filter_constant_column_after_where_decimal;

CREATE TABLE 04259_filter_constant_column_after_where_decimal
(
    d Decimal128(18)
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where_decimal VALUES
    (toDecimal128('1.000000000000000000', 18)),
    (toDecimal128('1.000000000000000001', 18)),
    (toDecimal128('2.000000000000000000', 18));

SELECT d, count()
FROM 04259_filter_constant_column_after_where_decimal
WHERE d = 1.0
GROUP BY ALL
ORDER BY ALL;

SELECT d, count()
FROM 04259_filter_constant_column_after_where_decimal
WHERE d = CAST(1.0, 'Dynamic')
GROUP BY ALL
ORDER BY ALL;

SELECT tupleElement(t, 1), count()
FROM
(
    SELECT tuple(arrayJoin([
        toDecimal128('1.000000000000000000', 18),
        toDecimal128('1.000000000000000001', 18),
        toDecimal128('2.000000000000000000', 18)])) AS t
)
WHERE t = tuple(1.0)
GROUP BY ALL
ORDER BY ALL;

SELECT dumpColumnStructure(t), tupleElement(t, 1), count()
FROM
(
    SELECT tuple(arrayJoin([
        toDecimal128('1.000000000000000000', 18),
        toDecimal128('1.000000000000000001', 18),
        toDecimal128('2.000000000000000000', 18)])) AS t
)
WHERE t = CAST(tuple(1.0), 'Dynamic')
GROUP BY ALL
ORDER BY ALL;

DROP TABLE IF EXISTS 04259_filter_constant_column_after_where_dynamic;

CREATE TABLE 04259_filter_constant_column_after_where_dynamic
(
    d Dynamic
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where_dynamic VALUES
    (CAST(reinterpretAsFloat64(toUInt64(0)), 'Dynamic')),
    (CAST(reinterpretAsFloat64(toUInt64(9223372036854775808)), 'Dynamic'));

SELECT reinterpretAsUInt64(dynamicElement(d, 'Float64'))
FROM 04259_filter_constant_column_after_where_dynamic
WHERE d = CAST(0.0, 'Dynamic')
ORDER BY reinterpretAsUInt64(dynamicElement(d, 'Float64'));

SELECT reinterpretAsUInt64(dynamicElement(tupleElement(t, 1), 'Float64'))
FROM
(
    SELECT tuple(d) AS t
    FROM 04259_filter_constant_column_after_where_dynamic
)
WHERE t = tuple(0.0)
ORDER BY reinterpretAsUInt64(dynamicElement(tupleElement(t, 1), 'Float64'));

DROP TABLE IF EXISTS 04259_filter_constant_column_after_where_variant;

SET allow_suspicious_variant_types = 1;

CREATE TABLE 04259_filter_constant_column_after_where_variant
(
    v Variant(Int8, UInt8)
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where_variant VALUES
    (CAST(1::Int8, 'Variant(Int8, UInt8)')),
    (CAST(1::UInt8, 'Variant(Int8, UInt8)')),
    (CAST(2::UInt8, 'Variant(Int8, UInt8)'));

SELECT variantType(v), count()
FROM 04259_filter_constant_column_after_where_variant
WHERE v = 1
GROUP BY ALL
ORDER BY ALL;

SELECT variantType(tupleElement(t, 1)), count()
FROM
(
    SELECT tuple(v) AS t
    FROM 04259_filter_constant_column_after_where_variant
)
WHERE t = tuple(1)
GROUP BY ALL
ORDER BY ALL;

DROP TABLE IF EXISTS 04259_filter_constant_column_after_where_fixed_string;

CREATE TABLE 04259_filter_constant_column_after_where_fixed_string
(
    s String
)
ENGINE = Memory;

INSERT INTO 04259_filter_constant_column_after_where_fixed_string VALUES
    (unhex('616263')),
    (unhex('61626300')),
    (unhex('6162630000')),
    (unhex('61626364'));

SELECT hex(s), length(s)
FROM 04259_filter_constant_column_after_where_fixed_string
WHERE s = toFixedString('abc', 5)
ORDER BY length(s), hex(s);

SELECT hex(tupleElement(t, 1)), length(tupleElement(t, 1))
FROM
(
    SELECT tuple(s) AS t
    FROM 04259_filter_constant_column_after_where_fixed_string
)
WHERE t = tuple(toFixedString('abc', 5))
ORDER BY length(tupleElement(t, 1)), hex(tupleElement(t, 1));

SELECT dumpColumnStructure(t), hex(tupleElement(t, 1)), length(tupleElement(t, 1))
FROM
(
    SELECT tuple(s) AS t
    FROM 04259_filter_constant_column_after_where_fixed_string
)
WHERE t = CAST(tuple(toFixedString('abc', 5)), 'Dynamic')
ORDER BY length(tupleElement(t, 1)), hex(tupleElement(t, 1));

DROP TABLE 04259_filter_constant_column_after_where;
DROP TABLE 04259_filter_constant_column_after_where_decimal;
DROP TABLE 04259_filter_constant_column_after_where_dynamic;
DROP TABLE 04259_filter_constant_column_after_where_variant;
DROP TABLE 04259_filter_constant_column_after_where_fixed_string;
