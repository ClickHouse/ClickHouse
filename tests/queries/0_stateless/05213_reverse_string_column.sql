-- Exercise every short String length in one ColumnString, including empty values and embedded zero bytes.
SELECT hex(reverse(value))
FROM
(
    SELECT arrayJoin(arrayMap(len -> substring(unhex('000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F2021'), 1, len), range(34))) AS value
);

-- Exercise variable row offsets and embedded zero bytes in longer Strings.
SELECT hex(reverse(value))
FROM
(
    SELECT arrayJoin([
        unhex('0102030405060708090A0B0C0D0E0F001112131415161718191A1B1C1D1E1F2021'),
        unhex('2122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142')
    ]) AS value
);
