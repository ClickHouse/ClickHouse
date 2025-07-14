-- Tags: no-fasttest

-- baseline test, encode of value should match expected val
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- different hrp value should yield a different result
select bech32Encode('tb', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- exactly the max amount of characters (50) should work
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d45494'));
-- strange, but valid
select bech32Encode('bcrt', unhex(''));

-- test other hrps
select bech32Encode('bcrt', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
select bech32Encode('tltc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
select bech32Encode('tltssdfsdvjnasdfnjkbhksdfasnbdfkljhaksdjfnakjsdhasdfnasdkfasdfasdfasdf', unhex('751e'));
-- too many chars
select bech32Encode('tltssdfsdvjnasdfnjkbhksdfasnbdfkljhaksdjfnakjsdhasdfnasdkfasdfasdfasdfdljsdfasdfahc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- negative tests
-- empty hrp
select bech32Encode('', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
select bech32Encode('bc', unhex('751E76E8199196D454941C45D1B3A323F1433BD6'));
-- 51 chars should return nothing
select bech32Encode('', unhex('751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d45494a'));

-- test with explicit witver = 1, should be same as default
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1) ==
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- testing old bech32 algo
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0);

-- witversions >=1 should all be the same
select bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1) ==
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 10);

-- roundtrip
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'))) as tup);

DROP TABLE IF EXISTS hex_data;
CREATE TABLE hex_data
(
    hrp String,
    data String,
    witver UInt8
)
ENGINE = Memory;

INSERT INTO hex_data VALUES
    ('bc', '6687112a6eadb4d88d29c7a45da56eff0c23b0e14e757d408e', 0),
    ('tb', '8f8cdd4364bb7dca11c49743da2c4b54062fa0388bbf924078', 1),
    ('bc', '50b80d45cc275f36eb5fb2c22a93f6a4e83ba9380e55c67f6a', 15),
    ('tb', 'b103a1937c6e2fb9de707a4be02d5d39e217b4bca7ce3c9c12', 0),
    ('bcrt', '95eb334ff82ef8ad76151c29094abdae6c9e8bb8244523e347', 2);

-- test const hrp with column data
SELECT bech32Encode('bc', unhex(data)) FROM hex_data limit 1;

-- test const data with column hrp
SELECT bech32Encode(hrp, unhex('6687112a6eadb4d88d29c7a45da56eff0c23b0e14e757d408e')) FROM hex_data limit 1;

-- test column hrp and data with const witver
SELECT bech32Encode(hrp, unhex(data), 1) FROM hex_data limit 1;

-- for encoding, if using a FixedString column for the data it is crucial that there is no padding
-- since the input is binary, there is no way to check for it
DROP TABLE IF EXISTS bech32_test;
CREATE TABLE bech32_test
(
    hrp String,
    data String,
    hrp_fixed FixedString(4),
    data_fixed FixedString(50),
    witver UInt8
)
ENGINE = Memory;

INSERT INTO bech32_test
SELECT hrp, data, CAST(hrp, 'FixedString(4)'), CAST(data, 'FixedString(50)'), witver
FROM hex_data;

SELECT
    bech32Encode(hrp, unhex(data)) AS enc,
    bech32Encode(hrp, unhex(data), witver) AS enc_witver,
    bech32Encode(hrp, unhex(data), witver) = bech32Encode(hrp_fixed, unhex(data_fixed), witver) AS match1,
    bech32Encode(hrp, unhex(data), witver) = bech32Encode(hrp, unhex(data_fixed), witver) AS match2,
    bech32Encode(hrp, unhex(data), witver) = bech32Encode(hrp_fixed, unhex(data), witver) AS match3
FROM bech32_test;

SELECT
    hrp,
    data,
    hrp = tup.1 AS match_hrp,
    data = lower(hex(tup.2)) AS match_data
FROM
(
    SELECT
        hrp,
        data,
        bech32Decode(bech32Encode(hrp, unhex(data), witver)) as tup
    FROM bech32_test
) AS round_trip;

DROP TABLE hex_data;
DROP TABLE bech32_test;

-- negative tests
select bech32Decode('');
select bech32Decode('foo');

-- decode valid string, witver 0, hrp=bc
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z') as tup);
-- decode valid string, witver 1, hrp=tb
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v') as tup);
-- decode valid string, witver 1, hrp=bc, should be same as witver 0 since same string was used to encode them
select bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7k8zcwmq') ==
       bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z');
-- decode valid string, witver 1, hrp=tb, see above comment
select bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7khalasw') ==
       bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v');

-- testing max length, this should work
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('b1w508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5xgqsaanm') as tup);
-- testing max length, this should returun nothing
select tup.1 as hrp, hex(tup.2) as data from (select bech32Decode('b1w508dfqejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5xgqsaanm') as tup);

-- test decode from table
DROP TABLE IF EXISTS addresses;
CREATE TABLE addresses
(
    address String
)
ENGINE = Memory;

INSERT INTO addresses VALUES
    ('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z'),
    ('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v'),
    ('tb1w508d6qejxtdg4y5r3zarvary0c5xw7khalasw'),
    ('bc1w508d6qejxtdg4y5r3zarvary0c5xw7k8zcwmq');

-- test that fixed strings give same result as regular string column
DROP TABLE IF EXISTS bech32_test;
CREATE TABLE bech32_test
(
    address String,
    address_fixed FixedString(45)
)
ENGINE = Memory;

INSERT INTO bech32_test
SELECT address, CAST(address, 'FixedString(45)')
FROM addresses;

SELECT
    address,
    bech32Decode(address).1 AS hrp,
    hex(bech32Decode(address).2) AS decoded,
    hex(bech32Decode(address_fixed).2) AS decoded_fixed,
    hex(bech32Decode(address).2) = hex(bech32Decode(address_fixed).2) AS match
FROM bech32_test;

DROP TABLE addresses;
DROP TABLE bech32_test
