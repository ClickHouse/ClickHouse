SELECT 'btc';
SELECT base58Encode('\x00\x0b\xe3\xe1\xeb\xa1\x7a\x47\x3f\x89\xb0\xf7\xe8\xe2\x49\x40\xf2\x0a\xeb\x8e\xbc\xa7\x1a\x88\xfd\xe9\x5d\x4b\x83\xb7\x1a\x09') = '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';

SELECT '32-byte round-trip';
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20'))) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20');

SELECT '32-byte all zeros';
SELECT base58Encode(unhex('0000000000000000000000000000000000000000000000000000000000000000')) = '11111111111111111111111111111111';
SELECT base58Decode('11111111111111111111111111111111') = unhex('0000000000000000000000000000000000000000000000000000000000000000');

SELECT '32-byte all-FF';
SELECT length(base58Encode(unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'))) > 0;
SELECT base58Decode(base58Encode(unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'))) = unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF');

SELECT '32-byte leading zeros';
SELECT base58Decode(base58Encode(unhex('0000000000000000000000000000000000000000000000000000000000000001'))) = unhex('0000000000000000000000000000000000000000000000000000000000000001');
SELECT base58Decode(base58Encode(unhex('0000000000000000000000000000000000000000000000000000000000AB0001'))) = unhex('0000000000000000000000000000000000000000000000000000000000AB0001');
SELECT base58Decode(base58Encode(unhex('00000000000000000000000000000000000000000000000000000000000000AB'))) = unhex('00000000000000000000000000000000000000000000000000000000000000AB');

SELECT '64-byte round-trip';
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40'))) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40');

SELECT '64-byte all zeros';
SELECT base58Encode(unhex('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')) = '1111111111111111111111111111111111111111111111111111111111111111';
SELECT base58Decode('1111111111111111111111111111111111111111111111111111111111111111') = unhex('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

SELECT '64-byte all-FF';
SELECT base58Decode(base58Encode(unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'))) = unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF');

SELECT '64-byte leading zeros';
SELECT base58Decode(base58Encode(unhex('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001'))) = unhex('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001');

-- Overflow: 44 z's exceeds 2^256, so it decodes to more than 32 bytes and must still round-trip
SELECT 'large decoded values';
SELECT length(base58Decode(repeat('z', 44))) > 32;
SELECT base58Decode(base58Encode(base58Decode(repeat('z', 44)))) = base58Decode(repeat('z', 44));
SELECT length(base58Decode(repeat('z', 88))) > 64;
SELECT base58Decode(base58Encode(base58Decode(repeat('z', 88)))) = base58Decode(repeat('z', 88));

-- Inputs whose encoded length falls in [32,44] or [64,88] without decoding to 32 or 64 bytes
-- must still round-trip when no expected size is given.
SELECT 'non-32/64 byte decode';
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718'))) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718'); -- 24 bytes
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f10111213141516171819'))) = unhex('0102030405060708090a0b0c0d0e0f10111213141516171819'); -- 25 bytes
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e'))) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e'); -- 30 bytes
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'))) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'); -- 31 bytes
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30313233343536373839'))) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30313233343536373839'); -- 50 bytes

SELECT 'invalid characters';
SELECT tryBase58Decode('0111111111111111111111111111111111111111111') = '';
SELECT tryBase58Decode('I1111111111111111111111111111111111111111111') = '';
SELECT tryBase58Decode('O111111111111111111111111111111111111111111111111111111111111111111111111111111111111111') = '';

SELECT 'decode errors';
SELECT base58Decode('!@#$%^&*()'); -- { serverError INCORRECT_DATA }
SELECT base58Decode('0invalid'); -- { serverError INCORRECT_DATA }

SELECT 'generic path';
SELECT base58Encode('Hello world!') = '2NEpo7TZRhna7vSvL';
SELECT base58Decode('2NEpo7TZRhna7vSvL') = 'Hello world!';
SELECT base58Encode('foobar') = 't1Zv2yaZ';
SELECT base58Decode('t1Zv2yaZ') = 'foobar';
SELECT base58Encode('') = '';
SELECT base58Decode('') = '';

SELECT 'tryBase58Decode';
SELECT tryBase58Decode('invalid!chars') = '';
SELECT tryBase58Decode('') = '';

SELECT 'size hint 32';
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20')), 32) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20');
SELECT base58Decode('11111111111111111111111111111111', 32) = unhex('0000000000000000000000000000000000000000000000000000000000000000');

SELECT 'size hint 64';
SELECT base58Decode(base58Encode(unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40')), 64) = unhex('0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40');
SELECT base58Decode('1111111111111111111111111111111111111111111111111111111111111111', 64) = unhex('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

SELECT 'size hint overflow';
SELECT base58Decode(repeat('z', 44), 32); -- { serverError INCORRECT_DATA }
SELECT base58Decode(repeat('z', 88), 64); -- { serverError INCORRECT_DATA }

SELECT 'size hint rejects any other decoded length';
SELECT base58Decode(repeat('1', 31), 32); -- { serverError INCORRECT_DATA }
SELECT base58Decode(repeat('1', 45), 32); -- { serverError INCORRECT_DATA }
SELECT base58Decode(concat(repeat('1', 32), '2'), 32); -- { serverError INCORRECT_DATA }
SELECT base58Decode(repeat('1', 63), 64); -- { serverError INCORRECT_DATA }
SELECT base58Decode(repeat('1', 89), 64); -- { serverError INCORRECT_DATA }
SELECT tryBase58Decode(repeat('1', 31), 32) = '';
SELECT tryBase58Decode(repeat('1', 63), 64) = '';

SELECT 'tryBase58Decode with size hint';
SELECT tryBase58Decode(repeat('z', 44), 32) = '';
SELECT tryBase58Decode(repeat('z', 88), 64) = '';
SELECT tryBase58Decode('invalid!chars', 32) = '';

-- The requirement holds at every size, not only at the two the removed fixed-size decoders served.
-- The first pair is the asymmetry that used to exist: both decode to one byte, and both are rejected.
SELECT 'expected size applies to every size';
SELECT base58Decode('2', 32); -- { serverError INCORRECT_DATA }
SELECT base58Decode('2', 34); -- { serverError INCORRECT_DATA }
SELECT base58Decode('2', 1) = unhex('01');
SELECT base58Decode(base58Encode('Hello world!'), 12) = 'Hello world!';
SELECT base58Decode(base58Encode('Hello world!'), 11); -- { serverError INCORRECT_DATA }
SELECT base58Decode(base58Encode('Hello world!'), 99); -- { serverError INCORRECT_DATA }
SELECT base58Decode(repeat('1', 33), 33) = repeat(unhex('00'), 33);
SELECT base58Decode('2', 18446744073709551615); -- { serverError INCORRECT_DATA }
SELECT tryBase58Decode('2', 34) = '';

SELECT 'bulk round-trip';
SELECT sum(dec = rs) == 100 FROM (SELECT randomString(32) AS rs, base58Decode(base58Encode(rs)) AS dec FROM numbers(100));
SELECT count() FROM (SELECT base58Decode(base58Encode(rs)) AS dec, rs FROM (SELECT unhex(hex(randomFixedString(32))) AS rs FROM numbers(100)) WHERE dec = rs);
SELECT count() FROM (SELECT base58Decode(base58Encode(rs)) AS dec, rs FROM (SELECT unhex(hex(randomFixedString(64))) AS rs FROM numbers(100)) WHERE dec = rs);

SELECT 'bulk round-trip with size hint';
SELECT count() FROM (SELECT base58Decode(base58Encode(rs), 32) AS dec, rs FROM (SELECT unhex(hex(randomFixedString(32))) AS rs FROM numbers(100)) WHERE dec = rs);
SELECT count() FROM (SELECT base58Decode(base58Encode(rs), 64) AS dec, rs FROM (SELECT unhex(hex(randomFixedString(64))) AS rs FROM numbers(100)) WHERE dec = rs);

SELECT 'short path boundary';
SELECT base58Decode(base58Encode(unhex('010203'))) = unhex('010203');
SELECT base58Decode(base58Encode(unhex('0102030405060708'))) = unhex('0102030405060708');
SELECT base58Decode(base58Encode(unhex('010203040506070809'))) = unhex('010203040506070809');
SELECT base58Decode('jpXCZedGfVR') = unhex('010000000000000000');
SELECT base58Decode('zzzzzzzzzzz') = unhex('015AC264554F0327FF');

SELECT 'word storage threshold';
SELECT base58Decode(base58Encode(repeat('a', 466))) = repeat('a', 466);
SELECT base58Decode(base58Encode(repeat('a', 467))) = repeat('a', 467);
SELECT base58Decode(base58Encode(repeat('a', 468))) = repeat('a', 468);
SELECT base58Encode(base58Decode(repeat('z', 696))) = repeat('z', 696);
SELECT base58Encode(base58Decode(repeat('z', 697))) = repeat('z', 697);
SELECT base58Encode(base58Decode(repeat('z', 698))) = repeat('z', 698);
SELECT base58Decode(base58Encode(concat(unhex('00000000000000000000'), repeat('a', 467)))) = concat(unhex('00000000000000000000'), repeat('a', 467));

-- A length that cannot decode to the expected size is rejected before the conversion runs, so this
-- returns immediately rather than spending the quadratic cost of two million characters.
SELECT 'size hint rejects an impossible length without converting';
SET function_base58_max_input_size = 0;
SET max_execution_time = 3;
SELECT base58Decode(concat(repeat('z', 1000000), repeat('z', 1000000)), 32); -- { serverError INCORRECT_DATA }
SELECT base58Decode(concat(repeat('z', 1000000), repeat('z', 1000000)), 64); -- { serverError INCORRECT_DATA }
