-- `obfuscateQueryWithSeed` replaces the values of literals but keeps the structure of the
-- query, see the FunctionDocumentation of REGISTER_FUNCTION(obfuscateQuery) in
-- src/Functions/obfuscateQuery.cpp: "Replaces numeric and string literals with random
-- values", "Preserves the overall query structure and SQL syntax". A constant seed makes
-- the result deterministic, so the preserved structure can be asserted exactly.

-- Hexadecimal integers: the 0x / 0X prefix is kept verbatim and every hex digit is replaced
-- by a hex digit, so the number of digits does not change.
SELECT match(obfuscateQueryWithSeed('SELECT 0xFF', 1), '^SELECT 0x[0-9A-F]{2}$') AS hex_prefix_lower,
       match(obfuscateQueryWithSeed('SELECT 0XAB', 1), '^SELECT 0X[0-9A-F]{2}$') AS hex_prefix_upper,
       obfuscateQueryWithSeed('SELECT 0xFF', 1) != 'SELECT 0xFF' AS hex_value_changed;

-- Hexadecimal floats keep the fraction point, the binary exponent marker and its sign.
SELECT match(obfuscateQueryWithSeed('SELECT 0x1.8p+3', 1), '^SELECT 0x[0-9A-F]\\.[0-9A-F]p\\+[0-9]$') AS hex_float_plus_exponent,
       match(obfuscateQueryWithSeed('SELECT 0xA.Bp-12', 1), '^SELECT 0x[0-9A-F]\\.[0-9A-F]p-[0-9]{2}$') AS hex_float_minus_exponent;

-- Binary integers keep the 0b / 0B prefix and stay binary.
SELECT match(obfuscateQueryWithSeed('SELECT 0b1010', 1), '^SELECT 0b[01]{4}$') AS bin_prefix_lower,
       match(obfuscateQueryWithSeed('SELECT 0B11', 1), '^SELECT 0B[01]{2}$') AS bin_prefix_upper;

-- UUIDs keep the 8-4-4-4-12 shape and the case of every hex digit, so an upper-case UUID is
-- replaced by upper-case hex only and a lower-case one by lower-case hex only.
SELECT match(obfuscateQueryWithSeed('SELECT ''AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE''', 1),
             '^SELECT ''[A-F]{8}-[A-F]{4}-[A-F]{4}-[A-F]{4}-[A-F]{12}''$') AS uuid_upper_case_kept,
       match(obfuscateQueryWithSeed('SELECT ''aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee''', 1),
             '^SELECT ''[a-f]{8}-[a-f]{4}-[a-f]{4}-[a-f]{4}-[a-f]{12}''$') AS uuid_lower_case_kept;

-- A real IPv4 address keeps its four groups, and the octets 0, 1 and 255 are kept as is.
SELECT match(obfuscateQueryWithSeed('SELECT ''192.168.1.255''', 1),
             '^SELECT ''[0-9]{1,3}\\.[0-9]{1,3}\\.1\\.255''$') AS ipv4_shape_kept;

-- A dotted number with a fifth group is not an IPv4 address: the leading group goes through
-- the plain number path and only the last four groups through the address path, so the result
-- still has five groups and each of them is a valid octet.
WITH obfuscateQueryWithSeed('SELECT ''5.6.7.8.9''', 1) AS o,
     splitByChar('.', extract(o, '[0-9.]+')) AS groups
SELECT length(groups) AS five_groups,
       arrayAll(x -> toUInt16(x) <= 255, groups) AS every_group_is_an_octet,
       o != 'SELECT ''5.6.7.8.9''' AS dotted_value_changed;

-- A number literal that does not fit into UInt64 (2^65 here) wraps to zero while being read
-- and is then replaced by a plain 64-bit value, which does fit into UInt64.
WITH obfuscateQueryWithSeed('SELECT 36893488147419103232', 1) AS o
SELECT match(o, '^SELECT [0-9]{1,20}$') AS still_a_decimal_number,
       toUInt64OrNull(extract(o, '[0-9]+')) IS NOT NULL AS replacement_fits_uint64,
       toUInt64OrNull('36893488147419103232') IS NULL AS input_does_not_fit_uint64;

-- Multi-byte UTF-8 inside a string literal is obfuscated byte by byte: a leading byte is
-- replaced by a leading byte of the same width and a continuation byte by a continuation
-- byte. The payload is U+00E9, U+20AC and U+1F600, that is a two-, a three- and a four-byte
-- sequence between two ASCII letters, so the byte length and the width of every code point
-- must survive the obfuscation.
WITH concat('SELECT ''a', unhex('C3A9E282ACF09F9880'), 'b''') AS q,
     obfuscateQueryWithSeed(q, 1) AS o
SELECT length(o) = length(q) AS same_byte_length,
       arrayMap(i -> length(substringUTF8(o, i, 1)), range(1, lengthUTF8(o) + 1))
           = arrayMap(i -> length(substringUTF8(q, i, 1)), range(1, lengthUTF8(q) + 1)) AS same_code_point_widths,
       o != q AS utf8_value_changed;
