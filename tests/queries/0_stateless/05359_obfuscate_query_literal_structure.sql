-- `obfuscateQueryWithSeed` replaces the values of literals but keeps the structure of the
-- query, see the FunctionDocumentation of REGISTER_FUNCTION(obfuscateQuery) in
-- src/Functions/obfuscateQuery.cpp: "Replaces numeric and string literals with random
-- values", "Preserves the overall query structure and SQL syntax". A constant seed makes
-- the result deterministic, so the preserved structure can be asserted exactly.

-- Hexadecimal integers: the 0x / 0X prefix is kept verbatim and every hex digit is replaced
-- by a hex digit, so the number of digits does not change. The replacement digits are always
-- taken from the upper-case table "0123456789ABCDEF", so lower-case input digits make the
-- regexes reject a literal that was passed through unchanged, without depending on any
-- particular obfuscated value.
SELECT match(obfuscateQueryWithSeed('SELECT 0xff', 1), '^SELECT 0x[0-9A-F]{2}$') AS hex_prefix_lower,
       match(obfuscateQueryWithSeed('SELECT 0Xab', 1), '^SELECT 0X[0-9A-F]{2}$') AS hex_prefix_upper;

-- Hexadecimal floats keep the fraction point, the binary exponent marker and its sign.
SELECT match(obfuscateQueryWithSeed('SELECT 0x1.8p+3', 1), '^SELECT 0x[0-9A-F]\\.[0-9A-F]p\\+[0-9]$') AS hex_float_plus_exponent,
       match(obfuscateQueryWithSeed('SELECT 0xA.Bp-12', 1), '^SELECT 0x[0-9A-F]\\.[0-9A-F]p-[0-9]{2}$') AS hex_float_minus_exponent;

-- Binary integers keep the 0b / 0B prefix and stay binary.
SELECT match(obfuscateQueryWithSeed('SELECT 0b1010', 1), '^SELECT 0b[01]{4}$') AS bin_prefix_lower,
       match(obfuscateQueryWithSeed('SELECT 0B11', 1), '^SELECT 0B[01]{2}$') AS bin_prefix_upper;

-- UUIDs keep the 8-4-4-4-12 shape and the character class of every single hex digit: an
-- upper-case letter is replaced by an upper-case letter, a lower-case letter by a lower-case
-- letter and a decimal digit by a decimal digit. The third literal mixes all three classes, so
-- each part of its regex pins one class at a known position: `01234567` must stay all digits,
-- `89AB` two digits then two upper-case letters, `CDEF` upper case, `abcd` lower case and
-- `ef0123456789` two lower-case letters then ten digits.
SELECT match(obfuscateQueryWithSeed('SELECT ''AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE''', 1),
             '^SELECT ''[A-F]{8}-[A-F]{4}-[A-F]{4}-[A-F]{4}-[A-F]{12}''$') AS uuid_upper_case_kept,
       match(obfuscateQueryWithSeed('SELECT ''aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee''', 1),
             '^SELECT ''[a-f]{8}-[a-f]{4}-[a-f]{4}-[a-f]{4}-[a-f]{12}''$') AS uuid_lower_case_kept,
       match(obfuscateQueryWithSeed('SELECT ''01234567-89AB-CDEF-abcd-ef0123456789''', 1),
             '^SELECT ''[0-9]{8}-[0-9]{2}[A-F]{2}-[A-F]{4}-[a-f]{4}-[a-f]{2}[0-9]{10}''$') AS uuid_digit_slots_stay_digits;

-- A `YYYY-MM-DD` literal keeps both dashes and the 4-2-2 digit groups and stays a valid date.
-- A `YYYY-MM-DD HH:MM:SS` literal keeps the space, both colons and the 2-2-2 digit groups of
-- the time, and the replacement is a valid wall clock time: the shape test of the time only
-- looks at the positions of the digits and the colons, so the impossible input time `99:99:99`
-- is accepted there and rewritten to an hour below 24 and a minute and a second below 60.
-- `parseDateTimeOrNull` is strict about the ranges, so it returns NULL for the input and a
-- value for the obfuscated literal only if the time was really rewritten. No assertion
-- compares the obfuscated value with the input: the date arm shifts the day by `hash % 256`
-- and the time arm draws from the whole day, so either may legitimately return the input.
WITH obfuscateQueryWithSeed('SELECT ''2023-04-05''', 1) AS d,
     obfuscateQueryWithSeed('SELECT ''2023-04-05 99:99:99''', 1) AS dt
SELECT match(d, '^SELECT ''[0-9]{4}-[0-9]{2}-[0-9]{2}''$') AS date_shape_kept,
       toDateOrNull(extract(d, '[0-9-]{10}')) IS NOT NULL AS date_still_parses,
       match(dt, '^SELECT ''[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}''$') AS datetime_shape_kept,
       parseDateTimeOrNull(extract(dt, '[0-9-]{10} [0-9:]{8}'), '%Y-%m-%d %H:%i:%S') IS NOT NULL AS datetime_still_parses,
       parseDateTimeOrNull('2023-04-05 99:99:99', '%Y-%m-%d %H:%i:%S') IS NULL AS input_time_is_not_a_valid_time;

-- A real IPv4 address keeps its four groups, and the octets 0, 1 and 255 are kept as is. The
-- address path parses every octet and writes it back with `writeIntText`, which never emits a
-- leading zero, while the plain number path copies a leading digit `0` verbatim and would keep
-- `010` as `010`. The first octet therefore tells the two paths apart structurally, and the
-- other three octets `255`, `1` and `255` must survive verbatim. The first octet is 10, which is
-- none of the preserved values, so its replacement is always drawn from 2..255; only that range
-- is a contract, never the value.
WITH obfuscateQueryWithSeed('SELECT ''010.255.1.255''', 1) AS o,
     splitByChar('.', extract(o, '[0-9.]+')) AS groups
SELECT match(o, '^SELECT ''[0-9]{1,3}\\.255\\.1\\.255''$') AS ipv4_shape_and_sentinel_octets_kept,
       NOT startsWith(groups[1], '0') AS first_octet_has_no_leading_zero,
       toUInt16(groups[1]) BETWEEN 2 AND 255 AS first_octet_in_range;

-- A dotted number with a fifth group is not an IPv4 address: the leading group goes through
-- the plain number path and only the last four groups through the address path. The two paths
-- are told apart structurally: the address path parses every octet and writes it back with
-- `writeIntText`, so a leading zero cannot survive it, while the number path copies the digits
-- `0` and `1` verbatim and would therefore keep `010` as `010`. The address path also keeps the
-- octets 0, 1 and 255 as is. So in `9.255.010.1.255` the trailing `255`, `1` and `255` must
-- survive verbatim, the `010` must come back without its leading zero, and the leading `9` must
-- stay a 4-bit number (the number path emits 8 + hash % 8, so it may map `9` back to `9`; only
-- the range is a contract).
WITH obfuscateQueryWithSeed('SELECT ''9.255.010.1.255''', 1) AS o,
     splitByChar('.', extract(o, '[0-9.]+')) AS groups
SELECT length(groups) AS five_groups,
       arrayAll(x -> toUInt16(x) <= 255, groups) AS every_group_is_an_octet,
       groups[2] = '255' AND groups[4] = '1' AND groups[5] = '255' AS trailing_sentinel_octets_kept,
       NOT startsWith(groups[3], '0') AS leading_zero_octet_canonicalised,
       toUInt16(groups[1]) BETWEEN 8 AND 15 AS leading_group_from_number_path;

-- A number literal that does not fit into UInt64 (2^65 here) wraps to zero while being read
-- and is then replaced by a plain 64-bit value, which does fit into UInt64.
WITH obfuscateQueryWithSeed('SELECT 36893488147419103232', 1) AS o
SELECT match(o, '^SELECT [0-9]{1,20}$') AS still_a_decimal_number,
       toUInt64OrNull(extract(o, '[0-9]+')) IS NOT NULL AS replacement_fits_uint64,
       toUInt64OrNull('36893488147419103232') IS NULL AS input_does_not_fit_uint64;

-- Non-ASCII bytes in a string literal are obfuscated byte by byte, and only the byte class is
-- preserved: a continuation byte (0x80..0xBF) stays a continuation byte, and a leading byte
-- stays a leading byte of the same width bucket (0xC0..0xDF, 0xE0..0xEF, 0xF0..0xF7). The
-- obfuscated bytes need not form valid UTF-8, so this is asserted on the raw bytes only.
-- The payload is U+00E9, U+20AC and U+1F600, that is a two-, a three- and a four-byte sequence,
-- so all three leading-byte buckets occur. It holds no ASCII character, and the assertions look
-- only at the bytes between the quotes, so nothing here can be satisfied by the obfuscation of
-- an ASCII word.
WITH concat('SELECT ''', unhex('C3A9E282ACF09F9880'), '''') AS q,
     obfuscateQueryWithSeed(q, 1) AS o,
     substring(q, 9, length(q) - 9) AS q_payload,
     substring(o, 9, length(o) - 9) AS o_payload,
     arrayMap(i -> reinterpretAsUInt8(substring(q_payload, i, 1)), range(1, length(q_payload) + 1)) AS q_bytes,
     arrayMap(i -> reinterpretAsUInt8(substring(o_payload, i, 1)), range(1, length(o_payload) + 1)) AS o_bytes,
     arrayMap(b -> multiIf(b < 0x80, 0, b < 0xC0, 1, b < 0xE0, 2, b < 0xF0, 3, 4), q_bytes) AS q_classes,
     arrayMap(b -> multiIf(b < 0x80, 0, b < 0xC0, 1, b < 0xE0, 2, b < 0xF0, 3, 4), o_bytes) AS o_classes
SELECT length(o) = length(q) AS same_byte_length,
       q_classes = [2, 1, 3, 1, 1, 4, 1, 1, 1] AS input_has_all_three_lead_widths,
       o_classes = q_classes AS same_byte_classes,
       startsWith(o, 'SELECT ''') AND endsWith(o, '''') AS quoting_kept_verbatim,
       o_payload != q_payload AS payload_bytes_changed;
