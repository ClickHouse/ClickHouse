SELECT hex(reverse(unhex('')));
SELECT hex(reverse(unhex('01')));
SELECT hex(reverse(unhex('0102')));
SELECT hex(reverse(unhex('010203')));
SELECT hex(reverse(unhex('01020304')));
SELECT hex(reverse(unhex('0102030405')));
SELECT hex(reverse(unhex('01020304050607')));
SELECT hex(reverse(unhex('0102030405060708')));
SELECT hex(reverse(unhex('010203040506070809')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F10')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F1011')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F101112131415161718')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F')));
SELECT hex(reverse(toFixedString(unhex('01'), 1)));
SELECT hex(reverse(toFixedString(unhex('0102'), 2)));
SELECT hex(reverse(toFixedString(unhex('010203'), 3)));
SELECT hex(reverse(toFixedString(unhex('01020304'), 4)));
SELECT hex(reverse(toFixedString(unhex('0102030405'), 5)));
SELECT hex(reverse(toFixedString(unhex('010203040506'), 6)));
SELECT hex(reverse(toFixedString(unhex('01020304050607'), 7)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708'), 8)));
SELECT hex(reverse(toFixedString(unhex('010203040506070809'), 9)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F'), 15)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F10'), 16)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F1011'), 17)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F101112131415161718'), 24)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F'), 31)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20'), 32)));
SELECT hex(reverse(toFixedString(unhex('0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F2021'), 33)));

-- Exercise multiple FixedString rows with an embedded zero byte.
SELECT hex(reverse(value))
FROM
(
    SELECT arrayJoin([
        toFixedString(unhex('0102030405060708090A0B0C0D0E0F001112131415161718191A1B1C1D1E1F'), 31),
        toFixedString(unhex('202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E'), 31)
    ]) AS value
);
