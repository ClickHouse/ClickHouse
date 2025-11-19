-- Basic functionality tests
SELECT conv('10', 10, 2); -- 1010
SELECT conv('255', 10, 16); -- FF
SELECT conv('FF', 16, 10); -- 255
SELECT conv('1010', 2, 8); -- 12
SELECT conv('12', 8, 10); -- 10

-- Case insensitive hex
SELECT conv('ff', 16, 10); -- 255
SELECT conv('AbC', 16, 10); -- 2748

-- Negative numbers
SELECT conv('-10', 10, 2); -- 1111111111111111111111111111111111111111111111111111111111110110
SELECT conv('-255', 10, 16); -- FFFFFFFFFFFFFF01
SELECT conv('-1', 10, 16); -- FFFFFFFFFFFFFFFF

-- Edge cases with whitespace
SELECT conv('  123  ', 10, 16); -- 7B
SELECT conv(' -456 ', 10, 8); -- 1777777777777777777070

-- Zero and empty cases
SELECT conv('0', 10, 2); -- 0
SELECT conv('0', 16, 10); -- 0

-- Invalid characters (should stop at first invalid)
SELECT conv('123XYZ', 10, 16); -- 7B
SELECT conv('FF99GG', 16, 10); -- 65433
SELECT conv('1012', 2, 10); -- 5

-- Boundary bases
SELECT conv('10', 2, 36); -- 2
SELECT conv('ZZ', 36, 10); -- 1295
SELECT conv('10', 36, 2); -- 100100

-- Large numbers (test overflow handling)
SELECT conv('18446744073709551615', 10, 16);  -- Max UInt64
SELECT conv('FFFFFFFFFFFFFFFF', 16, 10);      -- Max UInt64 in hex -- 18446744073709551615
SELECT conv('999999999999999999999', 10, 16); -- Overflow case -- FFFFFFFFFFFFFFFF

-- Only whitespace test
SELECT conv(' ', 16, 10); -- 0

-- Numeric input types
SELECT conv(255, 10, 16); -- FF
SELECT conv(1010, 2, 10); -- 10
SELECT conv(-123, 10, 16); -- FFFFFFFFFFFFFF85

-- Test with different column types
SELECT conv(toString(number), 10, 16) FROM system.numbers LIMIT 5; -- 0 1 2 3 4
SELECT conv(number, 10, 2) FROM system.numbers WHERE number < 8;

-- Const column optimization test
SELECT conv('FF', 16, 10) FROM system.numbers LIMIT 3; -- 255 255 255



-- Mixed scenarios
SELECT conv(toString(number), 10, 36), conv(toString(number), 10, 2)
FROM system.numbers WHERE number BETWEEN 10 AND 15;
