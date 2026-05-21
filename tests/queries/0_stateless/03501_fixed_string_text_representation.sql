DROP TABLE IF EXISTS fixed_string_text_representation;

CREATE TABLE fixed_string_text_representation
(
    raw FixedString(3),
    id58 FixedString(32, 'Base58'),
    ids58 Array(FixedString(32, 'Base58')),
    id64 FixedString(32, 'Base64'),
    ids64 Array(FixedString(32, 'Base64')),
    id64url FixedString(32, 'Base64URL'),
    idhex FixedString(4, 'Hex')
)
ENGINE = Memory;

INSERT INTO fixed_string_text_representation VALUES
(
    'abc',
    '11111111111111111111111111111111',
    ['11111111111111111111111111111111'],
    'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
    ['AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='],
    'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    '0x01020304'
);

SELECT toTypeName(raw), toTypeName(id58), toTypeName(ids58), toTypeName(id64), toTypeName(ids64), toTypeName(id64url), toTypeName(idhex)
FROM fixed_string_text_representation;

SELECT id58, id64, id64url, idhex
FROM fixed_string_text_representation;

SELECT count()
FROM fixed_string_text_representation
WHERE id58 = '11111111111111111111111111111111';

SELECT count()
FROM fixed_string_text_representation
WHERE '11111111111111111111111111111111' IN
(
    SELECT id58 FROM fixed_string_text_representation
);

SELECT has(ids58, '11111111111111111111111111111111'), hasAny(ids58, ['11111111111111111111111111111111'])
FROM fixed_string_text_representation;

SELECT count()
FROM fixed_string_text_representation
WHERE id64 = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';

SELECT count()
FROM fixed_string_text_representation
WHERE id64url = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';

SELECT count()
FROM fixed_string_text_representation
WHERE idhex = '01020304';

-- { serverError INCORRECT_DATA }
SELECT CAST('not_base58' AS FixedString(32, 'Base58'));

-- { serverError INCORRECT_DATA }
SELECT CAST('1' AS FixedString(32, 'Base58'));

-- { serverError INCORRECT_DATA }
SELECT CAST('!!!!' AS FixedString(32, 'Base64'));

-- { serverError INCORRECT_DATA }
SELECT CAST('010203' AS FixedString(4, 'Hex'));

DROP TABLE fixed_string_text_representation;
