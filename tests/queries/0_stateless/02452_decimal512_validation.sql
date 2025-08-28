-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test validation for Decimal512

DROP TABLE IF EXISTS decimal512_test;

CREATE TABLE decimal512_test (d1 Decimal(100, 20), d2 Decimal(150, 30)) ENGINE = Memory;

-- Verify correct type names
SELECT toTypeName(d1), toTypeName(d2) FROM decimal512_test LIMIT 1;

-- Insert a large value that requires 512 bits
INSERT INTO decimal512_test (d1) VALUES ('12345678901234567890123456789012345678901234567890123456789012345678901234567890.12345678901234567890');

SELECT d1 FROM decimal512_test;

-- Test arithmetic and type promotion
SELECT toTypeName(d1 + d1) FROM decimal512_test;
SELECT d1 * 2 FROM decimal512_test;

-- Test with another high-precision decimal
UPDATE decimal512_test SET d2 = toDecimal512('1.1', 30);
SELECT toTypeName(d1 + d2) FROM decimal512_test;
SELECT d1 + d2 FROM decimal512_test;

-- Test casting
SELECT CAST(1 as Decimal(38,2)) + d1 FROM decimal512_test;

-- Test overflow. This multiplication should throw a DECIMAL_OVERFLOW exception.
SELECT d1 * toDecimal32(10, 0) FROM decimal512_test; -- { serverError DECIMAL_OVERFLOW }

DROP TABLE decimal512_test;

-- Test explicit type name
DROP TABLE IF EXISTS decimal512_explicit_test;
CREATE TABLE decimal512_explicit_test(d Decimal512(40)) ENGINE=Memory;
INSERT INTO decimal512_explicit_test VALUES ('123.456');
SELECT toTypeName(d), d FROM decimal512_explicit_test;
DROP TABLE IF EXISTS decimal512_explicit_test;
