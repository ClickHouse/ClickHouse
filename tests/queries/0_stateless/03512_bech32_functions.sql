-- Tags: no-fasttest

-- baseline test, encode of value should match expected val
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- different hrp value should yield a different result
SELECT bech32Encode('tb', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- exactly the max amount of characters (50) should work
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d45494'));
-- strange, but valid
SELECT bech32Encode('bcrt', unhex(''));

-- test other hrps
SELECT bech32Encode('bcrt', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
SELECT bech32Encode('tltc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
SELECT bech32Encode('tltssdfsdvjnasdfnjkbhksdfasnbdfkljhaksdjfnakjsdhasdfnasdkfasdfasdfasdf', unhex('751e'));

-- negative tests
-- too many chars
SELECT bech32Encode('tltssdfsdvjnasdfnjkbhksdfasnbdfkljhaksdjfnakjsdhasdfnasdkfasdfasdfasdfdljsdfasdfahc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- empty hrp
SELECT bech32Encode('', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
-- 51 chars should return nothing
SELECT bech32Encode('', unhex('751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d454941c45d1b3a323f1433bd6751e76e8199196d45494a'));

-- test with explicit witver = 1, should be same as default
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1) ==
       bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- testing old bech32 algo
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0);

-- different witvers will not match perfectly, but the encoded data should match, so we strip off the first 4 chars (hrp and prepended witver)
-- as well as the last 6 chars (checksum)
SELECT substring(s1, 5, -6) == substring(s2, 5, -6)
FROM
(
    SELECT
        bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 1) AS s1,
        bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 10) AS s2
);

-- roundtrip
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'))) AS tup);

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

-- sanity check, should return hrp and data used to create it
SELECT tup.1, hex(tup.2) FROM (SELECT bech32Decode('bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq') AS tup);

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
        bech32Decode(bech32Encode(hrp, unhex(data), witver)) AS tup
    FROM bech32_test
) AS round_trip;

DROP TABLE hex_data;
DROP TABLE bech32_test;

-- negative tests
SELECT bech32Decode('');
SELECT bech32Decode('foo');

-- decode valid string, witver 0, hrp=bc
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kj9wkru') AS tup);
-- decode valid string, witver 1, hrp=tb
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('tb1pw508d6qejxtdg4y5r3zarvary0c5xw7kcr49c0') AS tup);
-- decoding address created with same data but different witvers should be same
SELECT t1.1 != '', t1.1 == t2.1, t1.2 == t2.2 FROM (
	SELECT bech32Decode('bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4') AS t1,
           bech32Decode('bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kj9wkru') AS t2);
-- decoding address created with same data but different witvers should be same
SELECT t1.1 != '', t1.1 == t2.1, t1.2 == t2.2 FROM (
	SELECT bech32Decode('tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx') AS t1,
           bech32Decode('tb1pw508d6qejxtdg4y5r3zarvary0c5xw7kcr49c0') AS t2);

-- testing max length, this should work
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('b1pw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y565gdg8') AS tup);
-- testing max length, this should return nothing
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('b1w508dfqejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5r3zarvary0c5xw7kw508d6qejxtdg4y5xgqsaanm') AS tup);

-- test decode from table
DROP TABLE IF EXISTS addresses;
CREATE TABLE addresses
(
    address String
)
ENGINE = Memory;

INSERT INTO addresses VALUES
    ('bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4'),
    ('tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx'),
    ('bc1pw508d6qejxtdg4y5r3zarvary0c5xw7kj9wkru'),
    ('tb1pw508d6qejxtdg4y5r3zarvary0c5xw7kcr49c0');

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
