SELECT 'Negative';

DROP DICTIONARY IF EXISTS dict_neg;
DROP TABLE IF EXISTS dict_src_neg;

CREATE TABLE dict_src_neg
(
    id   UInt64,
    u64  UInt64,
    i32n Nullable(Int32)
) ENGINE = Memory;

INSERT INTO dict_src_neg VALUES
    (1, 7, NULL),
    (2, 42, -10);

CREATE DICTIONARY dict_neg
(
    id   UInt64,
    u64  UInt64,
    i32n Nullable(Int32)
)

PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src_neg'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT dictGetKeys(toString(number), 'u64', toUInt64(7)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGetKeys('dict_neg', toString(number), toUInt64(7)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGetKeys('dict_neg', 'no_such_attr', toUInt64(7)); -- { serverError ILLEGAL_COLUMN }

SELECT dictGetKeys('dict_neg', 'u64'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT dictGetKeys('dict_neg', 'u64', toUInt64(7), 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT dictGetKeys('dict_neg', 'i32n', tuple(number)) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGetKeys('non_a_dict_name', 'i32n', tuple(number)) FROM numbers(3); -- { serverError BAD_ARGUMENTS }

SELECT dictGetKeys('dict_neg', 'not_a_attr_col', tuple(number)) FROM numbers(3); -- { serverError ILLEGAL_COLUMN }

SELECT 'Simple Key';

DROP DICTIONARY IF EXISTS dict_simple_kv;
DROP TABLE IF EXISTS dict_src_simple_kv;

CREATE TABLE dict_src_simple_kv
(
    id   UInt64,
    attr Int32
) ENGINE = Memory;

INSERT INTO dict_src_simple_kv VALUES
    (1, 10),
    (2, 10),
    (3, 20);

CREATE DICTIONARY dict_simple_kv
(
    id   UInt64,
    attr Int32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src_simple_kv'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT dictGetKeys('dict_simple_kv', 'attr', toUInt32(10));

SELECT toTypeName(dictGetKeys('dict_simple_kv', 'attr', toUInt32(10)));

SELECT 'Complex Key with 2 elements';

DROP DICTIONARY IF EXISTS dict_complex2_kv;
DROP TABLE IF EXISTS dict_src_complex2_kv;

CREATE TABLE dict_src_complex2_kv
(
    k1   UInt64,
    k2   String,
    attr Int32
) ENGINE = Memory;

INSERT INTO dict_src_complex2_kv VALUES
    (1, 'a', 10),
    (2, 'b', 10),
    (3, 'c', 20);

CREATE DICTIONARY dict_complex2_kv
(
    k1   UInt64,
    k2   String,
    attr Int32
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'dict_src_complex2_kv'))
LIFETIME(0)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT dictGetKeys('dict_complex2_kv', 'attr', 10);
SELECT toTypeName(dictGetKeys('dict_complex2_kv', 'attr', 10));


SELECT 'Complex Key with 1 element';

DROP DICTIONARY IF EXISTS dict_complex1_kv;
DROP TABLE IF EXISTS dict_src_complex1_kv;

CREATE TABLE dict_src_complex1_kv
(
    k1   UInt64,
    attr Int32
) ENGINE = Memory;

INSERT INTO dict_src_complex1_kv VALUES
    (10, 1),
    (20, 1);

CREATE DICTIONARY dict_complex1_kv
(
    k1   UInt64,
    attr Int32
)
PRIMARY KEY k1
SOURCE(CLICKHOUSE(TABLE 'dict_src_complex1_kv'))
LIFETIME(0)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT dictGetKeys('dict_complex1_kv', 'attr', 1);
SELECT toTypeName(dictGetKeys('dict_complex1_kv', 'attr', 1));

SELECT 'Complex Key with many elements';

DROP DICTIONARY IF EXISTS dict_complex_wide_kv;
DROP TABLE IF EXISTS dict_src_complex_wide_kv;

CREATE TABLE dict_src_complex_wide_kv
(
    a1 UInt64,
    a2 Int64,
    a3 String,
    a4 Date,
    a5 UUID,
    a6 IPv4,
    a7 IPv6,
    a8 DateTime64(3),
    attr Int32
) ENGINE = Memory;

INSERT INTO dict_src_complex_wide_kv VALUES
    (1, -1, 'x', toDate('2000-01-02'), toUUID('01234567-89ab-cdef-0123-456789abcdef'),
     toIPv4('1.2.3.4'), toIPv6('2001:db8::1'), toDateTime64('2025-10-10 03:04:05', 3), 10),
    (2, -2, 'y', toDate('2000-01-03'), toUUID('89abcdef-0123-4567-89ab-cdef01234567'),
     toIPv4('5.6.7.8'), toIPv6('2001:db8::2'), toDateTime64('2025-10-10 03:04:05', 3), 20);

CREATE DICTIONARY dict_complex_wide_kv
(
    a1 UInt64,
    a2 Int64,
    a3 String,
    a4 Date,
    a5 UUID,
    a6 IPv4,
    a7 IPv6,
    a8 DateTime64(3),
    attr Int32
)

PRIMARY KEY a1, a2, a3, a4, a5, a6, a7, a8
SOURCE(CLICKHOUSE(TABLE 'dict_src_complex_wide_kv'))
LIFETIME(0)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT dictGetKeys('dict_complex_wide_kv', 'attr', 10);
SELECT toTypeName(dictGetKeys('dict_complex_wide_kv', 'attr', 10));


SELECT 'Attribute Types';

DROP DICTIONARY IF EXISTS dict_types;
DROP TABLE IF EXISTS dict_src_types;

CREATE TABLE dict_src_types
(
    id   UInt64,
    i8   Int8,
    u8   UInt8,
    i64  Int64,
    u64  UInt64,
    f32  Float32,
    f64  Float64,
    dec32 Decimal32(3),
    dec64 Decimal64(3),
    d    Date,
    dt   DateTime,
    dt64 DateTime64(3),
    uuid UUID,
    ip4  IPv4,
    ip6  IPv6,
    s    String,
    arr_u64    Array(UInt64),
    arr_nested Array(Array(UInt8)),
    n_i32 Nullable(Int32),
    n_str Nullable(String)
) ENGINE = Memory;

INSERT INTO dict_src_types VALUES
(1,
  toInt8(-128), toUInt8(0), toInt64(-9223372036854775808), toUInt64(0),
  toFloat32(-1.5), toFloat64(-1.5),
  toDecimal32(-123.456, 3), toDecimal64(-123.456, 3),
  toDate('2025-01-01'), toDateTime('2025-01-01 00:00:00'), toDateTime64('2025-01-01 00:00:00', 3),
  toUUID('00000000-0000-0000-0000-000000000000'),
  toIPv4('0.0.0.0'), toIPv6('::'),
  '', [], [], NULL, NULL),
(2,
  toInt8(0), toUInt8(127), toInt64(0), toUInt64(42),
  toFloat32(1.5), toFloat64(42.25),
  toDecimal32(1.234, 3), toDecimal64(42.500, 3),
  toDate('2000-01-02'), toDateTime('2000-01-02 03:04:05'), toDateTime64('2000-01-02 03:04:05', 3),
  toUUID('01234567-89ab-cdef-0123-456789abcdef'),
  toIPv4('1.2.3.4'), toIPv6('2001:db8::1'),
  'alpha', [1,2], [[1,2],[3]], 0, 'x'),
(3,
  toInt8(127), toUInt8(255), toInt64(9223372036854775807), toUInt64(18446744073709551615),
  CAST('inf' AS Float32), CAST('nan' AS Float64),
  toDecimal32(123.999, 3), toDecimal64(9999999.999, 3),
  toDate('2106-02-07'), toDateTime('2106-02-07 06:28:15'), toDateTime64('2106-02-07 06:28:15', 3),
  toUUID('89abcdef-0123-4567-89ab-cdef01234567'),
  toIPv4('255.255.255.255'), toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
  'beta', [9,8,7], [[4],[5,6]], NULL, 'y');

CREATE DICTIONARY dict_types
(
    id   UInt64,
    i8   Int8,
    u8   UInt8,
    i64  Int64,
    u64  UInt64,
    f32  Float32,
    f64  Float64,
    dec32 Decimal32(3),
    dec64 Decimal64(3),
    d    Date,
    dt   DateTime,
    dt64 DateTime64(3),
    uuid UUID,
    ip4  IPv4,
    ip6  IPv6,
    s    String,
    arr_u64    Array(UInt64),
    arr_nested Array(Array(UInt8)),
    n_i32 Nullable(Int32),
    n_str Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src_types'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'Small Integer';
SELECT dictGetKeys('dict_types', 'i8', '-128');
SELECT dictGetKeys('dict_types', 'i8', '0');
SELECT dictGetKeys('dict_types', 'i8', '127');

SELECT 'Unsigned Integer';
SELECT dictGetKeys('dict_types', 'u64', '0');
SELECT dictGetKeys('dict_types', 'u64', '18446744073709551615');

SELECT 'Floating Point';
SELECT dictGetKeys('dict_types', 'f32', '1.5');
SELECT dictGetKeys('dict_types', 'f32', 'inf');
SELECT dictGetKeys('dict_types', 'f32', inf);
SELECT dictGetKeys('dict_types', 'f32', '-inf');
SELECT dictGetKeys('dict_types', 'f64', '42.25');
SELECT dictGetKeys('dict_types', 'f64', 'nan');
SELECT dictGetKeys('dict_types', 'f64', nan);

SELECT 'Array';
SELECT dictGetKeys('dict_types', 'arr_u64', []);
SELECT dictGetKeys('dict_types', 'arr_u64', [1,2]);
SELECT dictGetKeys('dict_types', 'arr_nested', [[1,2],[3]]);

SELECT dictGetKeys('dict_types', 'arr_u64', '[]');
SELECT dictGetKeys('dict_types', 'arr_u64', '[1,2]');
SELECT dictGetKeys('dict_types', 'arr_nested', '[[1,2],[3]]');

SELECT 'Nullable';
SELECT dictGetKeys('dict_types', 'n_i32', NULL);
SELECT dictGetKeys('dict_types', 'n_i32', 0);
SELECT dictGetKeys('dict_types', 'n_str', NULL);
SELECT dictGetKeys('dict_types', 'n_str', 'x');

SELECT 'Dates, UUID, IPs, String, Decimal';
SELECT dictGetKeys('dict_types', 'd', '2000-01-02');
SELECT dictGetKeys('dict_types', 'dt', '2000-01-02 03:04:05');
SELECT dictGetKeys('dict_types', 'dt64', toDateTime64('1970-01-01 00:00:00', 3));
SELECT dictGetKeys('dict_types', 'uuid', '01234567-89ab-cdef-0123-456789abcdef');
SELECT dictGetKeys('dict_types', 'ip4', '1.2.3.4');
SELECT dictGetKeys('dict_types', 'ip6', '2001:db8::1');
SELECT dictGetKeys('dict_types', 's', '');
SELECT dictGetKeys('dict_types', 'dec32', '1.234');
SELECT dictGetKeys('dict_types', 'dec64', '42.500');


SELECT 'Value Types';

DROP DICTIONARY IF EXISTS dict_valexpr;
DROP TABLE IF EXISTS dict_src_valexpr;

CREATE TABLE dict_src_valexpr
(
    id    UInt64,
    s     String,
    i32n  Nullable(Int32),
    u64   UInt64
) ENGINE = Memory;

INSERT INTO dict_src_valexpr VALUES
    (1, 'alpha', 10, 42),
    (2, 'beta', 10, 100),
    (3, 'gamma', -5, 42),
    (4, 'alpha', NULL, 7);

CREATE DICTIONARY dict_valexpr
(
    id    UInt64,
    s     String,
    i32n  Nullable(Int32),
    u64   UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src_valexpr'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'Constant value';
SELECT dictGetKeys('dict_valexpr', 's', 'alpha');

SELECT 'Non-const vector';
SELECT dictGetKeys('dict_valexpr', 's', v)
FROM (SELECT arrayJoin(['alpha','beta','zzz']) AS v);

SELECT 'LowCardinality(String) constant';
SELECT dictGetKeys('dict_valexpr', 's', CAST('alpha' AS LowCardinality(String)));

SELECT 'LowCardinality(String) vector';
SELECT dictGetKeys('dict_valexpr', 's', CAST(v AS LowCardinality(String)))
FROM (SELECT arrayJoin(['alpha','beta','zzz']) AS v);

SELECT 'Nullable constant NULL and non-NULL';
SELECT dictGetKeys('dict_valexpr', 'i32n', CAST(NULL AS Nullable(Int32)));
SELECT dictGetKeys('dict_valexpr', 'i32n', CAST(10 AS Nullable(Int32)));
SELECT dictGetKeys('dict_valexpr', 'i32n', CAST('10' AS Nullable(String)));

SELECT dictGetKeys('dict_valexpr', 'u64', CAST(NULL AS Nullable(Int32))); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT dictGetKeys('dict_valexpr', 'u64', CAST(-42 AS Nullable(Int32))); -- { serverError CANNOT_CONVERT_TYPE }
SELECT dictGetKeys('dict_valexpr', 'u64', CAST('42' AS Nullable(String)));

SELECT 'Nullable vector with NULLs interleaved';
SELECT dictGetKeys('dict_valexpr', 'i32n', x)
FROM (SELECT arrayJoin([CAST(NULL AS Nullable(Int32)), CAST(10 AS Nullable(Int32)), CAST(NULL AS Nullable(Int32)), CAST(-5 AS Nullable(Int32))]) AS x);

SELECT 'Type-mismatch constant convertible (String -> UInt64)';
SELECT dictGetKeys('dict_valexpr', 'u64', '42');


SELECT 'Match Patterns';

DROP DICTIONARY IF EXISTS dict_match;
DROP TABLE IF EXISTS dict_src_match;

CREATE TABLE dict_src_match
(
    id  UInt64,
    grp String
) ENGINE = Memory;

INSERT INTO dict_src_match VALUES
    (1, 'A'), (2, 'A'), (3, 'B'), (4, 'C');

CREATE DICTIONARY dict_match
(
    id  UInt64,
    grp String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src_match'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'No matches at all (const and vector)';
SELECT dictGetKeys('dict_match', 'grp', 'Z');
SELECT dictGetKeys('dict_match', 'grp', v)
FROM (SELECT arrayJoin(['Z','Y']) AS v);

SELECT 'One match per input row';
SELECT dictGetKeys('dict_match', 'grp', v)
FROM (SELECT arrayJoin(['B','C']) AS v);

SELECT 'Many matches per row (shared attribute)';
SELECT dictGetKeys('dict_match', 'grp', 'A');
SELECT dictGetKeys('dict_match', 'grp', v)
FROM (SELECT arrayJoin(['A','B','A']) AS v);
