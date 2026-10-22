SET enable_analyzer = 1;
SET optimize_rewrite_like_perfect_affix = 0;
SET short_circuit_function_evaluation = 'enable';

-- Dictionary key conversions saturate timestamps outside the range of `DateTime`.
CREATE TABLE conversion_dates_source (k DateTime('UTC'), attr String) ENGINE = Memory;
INSERT INTO conversion_dates_source VALUES
    ('1970-01-01 00:00:00', 'single'),
    ('1970-01-01 00:00:01', 'many'),
    ('1970-01-01 00:00:02', 'many');
CREATE DICTIONARY conversion_dates (k DateTime('UTC'), attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'conversion_dates_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE conversion_dates_data (id UInt8, t DateTime64(0, 'UTC'), kt Tuple(DateTime64(0, 'UTC'))) ENGINE = Memory;
INSERT INTO conversion_dates_data VALUES
    (1, '1969-12-31 23:59:59', ('1969-12-31 23:59:59',)),
    (2, '1970-01-01 00:00:01', ('1970-01-01 00:00:01',)),
    (3, '2106-02-07 06:28:17', ('2106-02-07 06:28:17',));
SELECT 'date conversion';

SELECT id,
    dictGet('conversion_dates', 'attr', t) = 'single',
    dictGet('conversion_dates', 'attr', tuple(t)) = 'many',
    dictGet('conversion_dates', 'attr', kt) LIKE 'ma%'
FROM conversion_dates_data ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id,
    dictGet('conversion_dates', 'attr', t) = 'single',
    dictGet('conversion_dates', 'attr', tuple(t)) = 'many',
    dictGet('conversion_dates', 'attr', kt) LIKE 'ma%'
FROM conversion_dates_data ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

-- Query conversion settings do not change the internal conversion of dictionary keys.
CREATE TABLE conversion_ip_source (ip4 IPv4, ip6 IPv6, attr String) ENGINE = Memory;
INSERT INTO conversion_ip_source VALUES ('0.0.0.0', '::', 'hit');
CREATE DICTIONARY conversion_ip4 (ip4 IPv4, attr String DEFAULT '')
PRIMARY KEY ip4 SOURCE(CLICKHOUSE(TABLE 'conversion_ip_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY conversion_ip6 (ip6 IPv6, attr String DEFAULT '')
PRIMARY KEY ip6 SOURCE(CLICKHOUSE(TABLE 'conversion_ip_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE conversion_strings (id UInt8, s String, kt Tuple(String, String)) ENGINE = Memory;
INSERT INTO conversion_strings VALUES (1, 'bad', ('bad', 'a'));
SET cast_ipv4_ipv6_default_on_conversion_error = 1;
SET input_format_ipv4_default_on_conversion_error = 1;
SET input_format_ipv6_default_on_conversion_error = 1;
SELECT 'IP conversion settings';

SELECT dictGet('conversion_ip4', 'attr', tuple(s)) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_IPV4 }

SELECT dictGet('conversion_ip4', 'attr', tuple(s)) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_IPV4 }

SELECT dictGet('conversion_ip4', 'attr', tuple(s)) LIKE 'hi%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_IPV4 }

SELECT dictGet('conversion_ip4', 'attr', tuple(s)) LIKE 'hi%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_IPV4 }

SELECT dictGet('conversion_ip6', 'attr', tuple(s)) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_IPV6 }

SELECT dictGet('conversion_ip6', 'attr', tuple(s)) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_IPV6 }

SELECT dictGet('conversion_ip6', 'attr', tuple(s)) LIKE 'hi%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_IPV6 }

SELECT dictGet('conversion_ip6', 'attr', tuple(s)) LIKE 'hi%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_IPV6 }

SET cast_ipv4_ipv6_default_on_conversion_error = 0;
SET input_format_ipv4_default_on_conversion_error = 0;
SET input_format_ipv6_default_on_conversion_error = 0;
TRUNCATE TABLE conversion_strings;
INSERT INTO conversion_strings VALUES (1, '01 Jan 2026 00:00:00', ('01 Jan 2026 00:00:00', 'a'));
SET cast_string_to_date_time_mode = 'best_effort';
SELECT 'date parsing settings';

SELECT dictGet('conversion_dates', 'attr', s) = 'single' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_dates', 'attr', s) = 'single' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_dates', 'attr', s) LIKE 'si%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_dates', 'attr', s) LIKE 'si%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_TEXT }

SET cast_string_to_date_time_mode = 'basic';

-- An unparsable string throws for a nullable key too, with and without the rewrite: the accurate
-- conversion of the key does not turn it into a `NULL` that would find a stored `NULL` key.
CREATE TABLE conversion_null_source (k1 Nullable(UInt8), k2 Nullable(UInt8), s String, attr String) ENGINE = Memory;
INSERT INTO conversion_null_source VALUES (NULL, 1, 'a', 'hit'), (1, NULL, 'a', 'other'), (2, 2, 'b', 'many'), (3, 3, 'b', 'many');
CREATE DICTIONARY conversion_null_scalar (k1 Nullable(UInt8), attr String DEFAULT '')
PRIMARY KEY k1 SOURCE(CLICKHOUSE(TABLE 'conversion_null_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY conversion_null_first (k1 Nullable(UInt8), s String, attr String DEFAULT '')
PRIMARY KEY k1, s SOURCE(CLICKHOUSE(TABLE 'conversion_null_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY conversion_null_second (s String, k2 Nullable(UInt8), attr String DEFAULT '')
PRIMARY KEY s, k2 SOURCE(CLICKHOUSE(TABLE 'conversion_null_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
TRUNCATE TABLE conversion_strings;
INSERT INTO conversion_strings VALUES (1, 'bad', ('bad', 'a'));
SELECT 'nullable conversion, unparsable string';

SELECT dictGet('conversion_null_scalar', 'attr', s) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_null_scalar', 'attr', s) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_null_first', 'attr', (s, 'a')) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_null_first', 'attr', (s, 'a')) = 'hit' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_null_second', 'attr', ('a', s)) LIKE 'oth%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError CANNOT_PARSE_TEXT }

SELECT dictGet('conversion_null_second', 'attr', ('a', s)) LIKE 'oth%' FROM conversion_strings SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError CANNOT_PARSE_TEXT }

TRUNCATE TABLE conversion_strings;
INSERT INTO conversion_strings VALUES (2, '1', ('1', 'a')), (3, '2', ('2', 'b'));
SELECT 'nullable conversion results';

SELECT id,
    dictGet('conversion_null_scalar', 'attr', s) = 'hit',
    dictGet('conversion_null_scalar', 'attr', tuple(s)) = 'many',
    dictGet('conversion_null_scalar', 'attr', s) LIKE 'hi%',
    dictGet('conversion_null_first', 'attr', (s, 'a')) = 'hit',
    dictGet('conversion_null_first', 'attr', kt) LIKE 'hi%',
    dictGet('conversion_null_second', 'attr', ('a', s)) = 'other',
    dictGet('conversion_null_second', 'attr', ('a', s)) LIKE 'oth%'
FROM conversion_strings ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id,
    dictGet('conversion_null_scalar', 'attr', s) = 'hit',
    dictGet('conversion_null_scalar', 'attr', tuple(s)) = 'many',
    dictGet('conversion_null_scalar', 'attr', s) LIKE 'hi%',
    dictGet('conversion_null_first', 'attr', (s, 'a')) = 'hit',
    dictGet('conversion_null_first', 'attr', kt) LIKE 'hi%',
    dictGet('conversion_null_second', 'attr', ('a', s)) = 'other',
    dictGet('conversion_null_second', 'attr', ('a', s)) LIKE 'oth%'
FROM conversion_strings ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

-- A unique composite key containing `NULL` must retain membership semantics for misses.
-- The outer nullable tuple also preserves actual null probes in projections and negations.
CREATE TABLE conversion_null_probes (id UInt8, flag UInt8) ENGINE = Memory;
INSERT INTO conversion_null_probes VALUES (1, 1), (2, 1), (3, 0);

SET transform_null_in = 0;
SELECT 'composite null key, transform_null_in=0';

SELECT id, dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id, dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT id, dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id, dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 1;

SET transform_null_in = 1;
SELECT 'composite null key, transform_null_in=1';

SELECT id, dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id, dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_first', 'attr', if(flag, (id, 'a'), NULL)) = 'hit' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT id, dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id, dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p, NOT p, isNull(p) FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT countIf(NOT p), countIf(isNull(p)) FROM (SELECT dictGet('conversion_null_second', 'attr', if(flag, ('a', id), NULL)) = 'other' AS p FROM conversion_null_probes) SETTINGS optimize_inverse_dictionary_lookup = 1;

SET transform_null_in = 0;
-- Non-null tuple probes must also miss a unique key containing `NULL` without an exception.
SELECT 'non-null composite probe';

SELECT id, dictGet('conversion_null_first', 'attr', (id, 'a')) = 'hit' FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT id, dictGet('conversion_null_first', 'attr', (id, 'a')) = 'hit' FROM conversion_null_probes ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

-- Unsupported key conversions are evaluated only on rows reaching the lookup.
CREATE TABLE conversion_numbers_source (k UInt8, k2 UInt8, attr String) ENGINE = Memory;
INSERT INTO conversion_numbers_source VALUES (1, 1, 'single'), (2, 2, 'many'), (3, 3, 'many');
CREATE DICTIONARY conversion_numbers (k UInt8, attr String DEFAULT '')
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'conversion_numbers_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE DICTIONARY conversion_numbers_composite (k UInt8, k2 UInt8, attr String DEFAULT '')
PRIMARY KEY k, k2 SOURCE(CLICKHOUSE(TABLE 'conversion_numbers_source')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);
CREATE TABLE conversion_arrays (x Array(UInt64)) ENGINE = Memory;
SELECT 'empty input';

SELECT count() FROM conversion_arrays WHERE dictGet('conversion_numbers', 'attr', x) = 'single' SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT count() FROM conversion_arrays WHERE dictGet('conversion_numbers', 'attr', x) = 'single' SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT count() FROM conversion_arrays WHERE dictGet('conversion_numbers', 'attr', x) = 'many' SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT count() FROM conversion_arrays WHERE dictGet('conversion_numbers', 'attr', x) = 'many' SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT count() FROM conversion_arrays WHERE dictGet('conversion_numbers', 'attr', x) LIKE 'si%' SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT count() FROM conversion_arrays WHERE dictGet('conversion_numbers', 'attr', x) LIKE 'si%' SETTINGS optimize_inverse_dictionary_lookup = 1;

INSERT INTO conversion_arrays VALUES ([0]);
SELECT 'unselected lookup branches';

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', x) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', x) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', x) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', x) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', x) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', x) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', tuple(x)) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', tuple(x)) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', tuple(x)) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', tuple(x)) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', tuple(x)) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers', 'attr', tuple(x)) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (x, toUInt8(1))) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (x, toUInt8(1))) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (x, toUInt8(1))) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (x, toUInt8(1))) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (x, toUInt8(1))) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (x, toUInt8(1))) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (toUInt8(1), x)) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (toUInt8(1), x)) = 'single') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (toUInt8(1), x)) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (toUInt8(1), x)) = 'many') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (toUInt8(1), x)) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT if(x[1] = 0, 1, dictGet('conversion_numbers_composite', 'attr', (toUInt8(1), x)) LIKE 'si%') FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'evaluated unsupported conversion';

SELECT dictGet('conversion_numbers', 'attr', x) = 'single' FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGet('conversion_numbers', 'attr', x) = 'single' FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGet('conversion_numbers', 'attr', x) = 'many' FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGet('conversion_numbers', 'attr', x) = 'many' FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGet('conversion_numbers', 'attr', x) LIKE 'si%' FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT dictGet('conversion_numbers', 'attr', x) LIKE 'si%' FROM conversion_arrays SETTINGS optimize_inverse_dictionary_lookup = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP DICTIONARY conversion_dates;
DROP DICTIONARY conversion_ip4;
DROP DICTIONARY conversion_ip6;
DROP DICTIONARY conversion_null_scalar;
DROP DICTIONARY conversion_null_first;
DROP DICTIONARY conversion_null_second;
DROP DICTIONARY conversion_numbers;
DROP DICTIONARY conversion_numbers_composite;
DROP TABLE conversion_dates_source;
DROP TABLE conversion_dates_data;
DROP TABLE conversion_ip_source;
DROP TABLE conversion_strings;
DROP TABLE conversion_null_source;
DROP TABLE conversion_null_probes;
DROP TABLE conversion_numbers_source;
DROP TABLE conversion_arrays;
