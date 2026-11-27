-- Tags: no-fasttest
-- no-fasttest: the Arrow format is not available in fasttest builds

-- The requested-type hints of the native `Arrow` reader must resolve the same way at decode time and
-- in the post-decode raw-byte rewrite: by tuple element name, through `Nullable`/`LowCardinality`
-- wrappers, through the `Array` layers of a flattened `Nested` request, and for every numeric-like
-- `date32` target. Each round trip below writes with the ClickHouse Arrow writer (which stores `UUID`,
-- `IPv6` and the big integers as `fixed_size_binary`, and a `String`'s raw bytes as variable `binary`)
-- and reads back with a request that must resolve to the same conversion as its plain same-order form.

SET engine_file_truncate_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;

-- A named-tuple request pairs elements by name, so a reordered or subset request must convert the
-- raw-byte leaf exactly like the same-order request does.
INSERT INTO FUNCTION file(currentDatabase() || '_05023_struct.arrow', 'Arrow', 's Tuple(a IPv6, b Int32)')
SELECT (toIPv6('2001:db8::1'), 7);

-- A `LowCardinality` wrapper on the requested type asks for the same raw bytes as the plain type.
INSERT INTO FUNCTION file(currentDatabase() || '_05023_flat.arrow', 'Arrow', 'v IPv6')
SELECT toIPv6('2001:db8::2');
INSERT INTO FUNCTION file(currentDatabase() || '_05023_array.arrow', 'Arrow', 'a Array(IPv6)')
SELECT [toIPv6('2001:db8::3')];
-- The variable `binary` layout takes a different conversion path (the decode-time width sniff) than
-- the ClickHouse writer's `fixed_size_binary`; cover it with raw 16-byte `String` values.
INSERT INTO FUNCTION file(currentDatabase() || '_05023_binary.arrow', 'Arrow', 'v String')
SELECT unhex('20010db8000000000000000000000004');

-- A flattened `Nested` request names the element type wrapped in one `Array` per enclosing list;
-- the hint must reach the leaf as the element type, exactly as the explicit `Array(Tuple(...))`
-- request does.
INSERT INTO FUNCTION file(currentDatabase() || '_05023_nested_int128.arrow', 'Arrow', 'n Array(Tuple(v Int128))')
SELECT [CAST(tuple(toInt128(1234567890123456789)), 'Tuple(v Int128)')];
INSERT INTO FUNCTION file(currentDatabase() || '_05023_nested_date32.arrow', 'Arrow', 'n Array(Tuple(d Date32))')
SELECT [CAST(tuple(toDate32('9999-12-31') + 100), 'Tuple(d Date32)')];

-- `date32` under a Decimal target reads as the raw day number, like the integer and float targets:
-- there is no `Date32` -> Decimal cast, so the raw day number is the only value the request can mean.
INSERT INTO FUNCTION file(currentDatabase() || '_05023_date32.arrow', 'Arrow', 'd Date32')
SELECT toDate32('9999-12-31') + 100;
INSERT INTO FUNCTION file(currentDatabase() || '_05023_date32_inrange.arrow', 'Arrow', 'd Date32')
SELECT toDate32('2020-01-01');

-- { echoOn }

SELECT * FROM file(currentDatabase() || '_05023_struct.arrow', 'Arrow', 's Tuple(b Int32, a IPv6)');
SELECT * FROM file(currentDatabase() || '_05023_struct.arrow', 'Arrow', 's Tuple(a IPv6)');
SELECT * FROM file(currentDatabase() || '_05023_struct.arrow', 'Arrow', 's Tuple(a IPv6, b Int32)');

SELECT * FROM file(currentDatabase() || '_05023_flat.arrow', 'Arrow', 'v LowCardinality(IPv6)');
SELECT * FROM file(currentDatabase() || '_05023_array.arrow', 'Arrow', 'a Array(LowCardinality(IPv6))');
SELECT * FROM file(currentDatabase() || '_05023_binary.arrow', 'Arrow', 'v LowCardinality(IPv6)');

SELECT * FROM file(currentDatabase() || '_05023_nested_int128.arrow', 'Arrow', 'n Nested(v Int128)');
SELECT * FROM file(currentDatabase() || '_05023_nested_date32.arrow', 'Arrow', 'n Nested(d Int32)');

SELECT * FROM file(currentDatabase() || '_05023_date32.arrow', 'Arrow', 'd Decimal(10, 0)');
SELECT * FROM file(currentDatabase() || '_05023_date32_inrange.arrow', 'Arrow', 'd Decimal(10, 0)');
