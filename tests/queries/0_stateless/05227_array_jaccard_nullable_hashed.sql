-- Generic hashed types must count `NULL` separately from the nested type's default value.
SELECT arrayUniq([NULL, toDecimal32(0, 2)]);
SELECT arrayUniq([NULL, toUUID('00000000-0000-0000-0000-000000000000')]);
SELECT arrayUniq([NULL, toDateTime64(0, 3)]);
SELECT arrayUniq([NULL, tuple(toUInt8(0), '')]);

SELECT arrayEnumerateUniq([NULL, toDecimal32(0, 2), NULL, toDecimal32(0, 2)]);
SELECT arrayEnumerateDense([NULL, toDecimal32(0, 2), NULL, toDecimal32(0, 2)]);
SELECT arrayEnumerateUniq(materialize([
    NULL,
    toUUID('00000000-0000-0000-0000-000000000000'),
    NULL,
    toUUID('00000000-0000-0000-0000-000000000000')]));
SELECT arrayEnumerateDense(materialize([
    NULL,
    tuple(toUInt8(0), ''),
    NULL,
    tuple(toUInt8(0), '')]));

SELECT arrayUniq(x), arrayEnumerateUniq(x), arrayEnumerateDense(x)
FROM values('x Array(Nullable(Decimal32(2)))', ([NULL, 0]), ([0, NULL, NULL]), ([]));

-- Multiple arguments keep nullable columns as part of the hashed tuple key.
SELECT
    arrayUniq([NULL, NULL], [toDecimal32(1, 2), toDecimal32(2, 2)]),
    arrayEnumerateUniq([NULL, NULL], [toDecimal32(1, 2), toDecimal32(2, 2)]),
    arrayEnumerateDense([NULL, NULL], [toDecimal32(1, 2), toDecimal32(2, 2)]);

SELECT arrayJaccardIndex([NULL, toDecimal32(0, 2)], [NULL, toDecimal32(0, 2)]);
SELECT arrayJaccardIndex([NULL, toDecimal32(0, 2)], [toDecimal32(0, 2)]);
SELECT round(arrayJaccardIndex([NULL, toDecimal32(0, 2), toDecimal32(3, 2)], [NULL, toDecimal32(0, 2)]), 2);
SELECT arrayJaccardIndex(
    [NULL, toUUID('00000000-0000-0000-0000-000000000000')],
    [NULL, toUUID('00000000-0000-0000-0000-000000000000')]);
SELECT arrayJaccardIndex(
    materialize([NULL, toDecimal32(0, 2)]),
    materialize([NULL, toDecimal32(0, 2)]));
