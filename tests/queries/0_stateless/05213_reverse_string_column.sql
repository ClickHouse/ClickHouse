-- Exercise every short String length in one ColumnString, including empty values and embedded zero bytes.
SELECT hex(reverse(value))
FROM
(
    SELECT arrayJoin(arrayMap(len -> substring(unhex('000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F2021'), 1, len), range(34))) AS value
);
