SELECT base32Encode('This is a test string');

SELECT base32Encode('This is a test string', 'Second arg'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE IF EXISTS t3447;
CREATE TABLE t3447 (id Int32, str String, b32 String) ENGINE = Memory;
INSERT INTO t3447 VALUES
    (100, '', ''),
    (101, 'f', 'MY======'),
    (102, 'fo', 'MZXQ===='),
    (103, 'foo', 'MZXW6==='),
    (104, 'foob', 'MZXW6YQ='),
    (105, 'fooba', 'MZXW6YTB'),
    (106, 'foobar', 'MZXW6YTBOI======'),

    (200, '\x00', 'AA======'),
    (201, '\x00\x00', 'AAAA===='),
    (202, '\x00\x00\x00', 'AAAAA==='),
    (203, '\x00\x00\x00\x00', 'AAAAAAA='),
    (204, '\x00\x00\x00\x00\x00', 'AAAAAAAA'),

    (300, '\xFF', '74======'),
    (301, '\xFF\xFF', '777Q===='),
    (302, '\xFF\xFF\xFF', '77776==='),
    (303, '\xFF\xFF\xFF\xFF', '777777Y='),
    (304, '\xFF\xFF\xFF\xFF\xFF', '77777777'),

    (400, '\x01\x23\x45\x67\x89', 'AERUKZ4J'),
    (401, '\xAB\xCD\xEF\x01\x23', 'VPG66AJD'),

    (402, '1234567890', 'GEZDGNBVGY3TQOJQ'),
    (403, 'The quick brown fox jumps over the lazy dog', 'KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO==='),

    (500, 'a', 'ME======'),
    (501, 'ab', 'MFRA===='),
    (502, 'abc', 'MFRGG==='),
    (503, 'abcd', 'MFRGGZA='),
    (504, 'abcde', 'MFRGGZDF'),
    (505, 'abcdef', 'MFRGGZDFMY======'),
    (506, 'foo', 'MZXW6==='),
    (507, 'foobar', 'MZXW6YTBOI======'),
    (508, 'Hello world!', 'JBSWY3DPEB3W64TMMQQQ===='),
    (509, 'Hold my beer', 'JBXWYZBANV4SAYTFMVZA===='),
    (510, 'Hold another beer', 'JBXWYZBAMFXG65DIMVZCAYTFMVZA===='),
    (511, 'And a wine', 'IFXGIIDBEB3WS3TF'),
    (512, 'And another wine', 'IFXGIIDBNZXXI2DFOIQHO2LOMU======'),
    (513, 'And a lemonade', 'IFXGIIDBEBWGK3LPNZQWIZI='),
    (514, 't1Zv2yaZ', 'OQYVU5RSPFQVU==='),
    (515, 'And another wine', 'IFXGIIDBNZXXI2DFOIQHO2LOMU======');


SELECT 'Part 1 - Encoding';
SELECT id, str AS input, hex(str) AS input_hex, base32Encode(str) AS result, b32, result == b32 FROM t3447;

SELECT 'Part 2a - Decoding';
SELECT id, b32 as input, base32Decode(input) AS result, hex(result) as result_hex, hex(str) as expected_hex, result == str FROM t3447;

SELECT 'Part 2b - Decoding lowercase';
SELECT id, lower(b32) as input, base32Decode(input) AS result, hex(result) as result_hex, hex(str) as expected_hex, result == str FROM t3447;

SELECT 'Part 3 - Roundtrip';
SELECT id, str AS input, hex(str) AS input_hex, base32Decode(base32Encode(str)) AS result, result == str FROM t3447;

SELECT 'Part 4 - Invalid input';
SELECT base32Decode('========'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YT!'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6Y=B'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6Y=!'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6Y==='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ=Q'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ======'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('12345678'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ=='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ==='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ===='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ====='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ======'); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ======='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ====!=='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ====A=='); -- { serverError INCORRECT_DATA }
SELECT base32Decode('MZXW6YQ======'); -- { serverError INCORRECT_DATA }

SELECT 'Part 5 - tryBase32Decode';
SELECT tryBase32Decode('========');
SELECT tryBase32Decode('MZXW6YT!');
SELECT tryBase32Decode('MZXW6Y=B');
SELECT tryBase32Decode('MZXW6Y=!');
SELECT tryBase32Decode('MZXW6Y===');
SELECT tryBase32Decode('MZXW6YQ=Q');
SELECT tryBase32Decode('MZXW6YQ======');
SELECT tryBase32Decode('12345678');
SELECT tryBase32Decode('MZXW6YQ');
SELECT tryBase32Decode('MZXW6YQ==');
SELECT tryBase32Decode('MZXW6YQ===');
SELECT tryBase32Decode('MZXW6YQ====');
SELECT tryBase32Decode('MZXW6YQ=====');
SELECT tryBase32Decode('MZXW6YQ======');
SELECT tryBase32Decode('MZXW6YQ=======');
SELECT tryBase32Decode('MZXW6YQ====!==');
SELECT tryBase32Decode('MZXW6YQ====A==');
SELECT tryBase32Decode('MZXW6YQ======');

SELECT 'Part 6 - FixedString encoding + decoding';
SELECT val, hex(val), base32Encode(val) as enc_res, hex(base32Decode(enc_res)) as dec_res, dec_res == hex(val) FROM (SELECT arrayJoin([
    toFixedString('', 1),
    toFixedString('f', 1),
    toFixedString('fo', 2),
    toFixedString('foo', 3),
    toFixedString('foob', 4),
    toFixedString('fooba', 5),
    toFixedString('foobar', 6),
    toFixedString('\x00', 1),
    toFixedString('\x00\x00', 2),
    toFixedString('\x00\x00\x00', 3),
    toFixedString('\x00\x00\x00\x00', 4),
    toFixedString('\x00\x00\x00\x00\x00', 5),
    toFixedString('\xFF', 1),
    toFixedString('\xFF\xFF', 2),
    toFixedString('\xFF\xFF\xFF', 3),
    toFixedString('\xFF\xFF\xFF\xFF', 4),
    toFixedString('\xFF\xFF\xFF\xFF\xFF', 5),
    toFixedString('\x01\x23\x45\x67\x89', 5),
    toFixedString('\xAB\xCD\xEF\x01\x23', 5),
    toFixedString('1234567890', 10),
    toFixedString('The quick brown fox jumps over the lazy dog', 43),
    toFixedString('a', 1),
    toFixedString('ab', 2),
    toFixedString('abc', 3),
    toFixedString('abcd', 4),
    toFixedString('abcde', 5),
    toFixedString('abcdef', 6),
    toFixedString('foo', 3),
    toFixedString('foobar', 6),
    toFixedString('Hello world!', 12),
    toFixedString('Hold my beer', 12),
    toFixedString('Hold another beer', 18),
    toFixedString('And a wine', 10),
    toFixedString('And another wine', 17),
    toFixedString('And a lemonade', 14),
    toFixedString('t1Zv2yaZ', 8),
    toFixedString('And another wine', 17)
    ]) val);

SELECT 'Part 6 - FixedString decoding + encoding';
SELECT val, base32Decode(val) as dec_res, hex(dec_res), base32Encode(dec_res) as enc_res, enc_res == val FROM (SELECT arrayJoin([
    toFixedString('AAAAA===', 8),
    toFixedString('MY======', 8),
    toFixedString('MZXQ====', 8),
    toFixedString('MZXW6===', 8),
    toFixedString('MZXW6YQ=', 8),
    toFixedString('MZXW6YTB', 8),
    toFixedString('MZXW6YTBOI======', 16),
    toFixedString('AA======', 8),
    toFixedString('AAAA====', 8),
    toFixedString('AAAAA===', 8),
    toFixedString('AAAAAAA=', 8),
    toFixedString('AAAAAAAA', 8),
    toFixedString('74======', 8),
    toFixedString('777Q====', 8),
    toFixedString('77776===', 8),
    toFixedString('777777Y=', 8),
    toFixedString('77777777', 8),
    toFixedString('AERUKZ4J', 8),
    toFixedString('VPG66AJD', 8),
    toFixedString('GEZDGNBVGY3TQOJQ', 16),
    toFixedString('KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO===', 96),
    toFixedString('ME======', 8),
    toFixedString('MFRA====', 8),
    toFixedString('MFRGG===', 8),
    toFixedString('MFRGGZA=', 8),
    toFixedString('MFRGGZDF', 8),
    toFixedString('MFRGGZDFMY======', 16),
    toFixedString('MZXW6===', 8),
    toFixedString('MZXW6YTBOI======', 16),
    toFixedString('JBSWY3DPEB3W64TMMQQQ====', 24),
    toFixedString('JBXWYZBANV4SAYTFMVZA====', 24),
    toFixedString('JBXWYZBAMFXG65DIMVZCAYTFMVZA====', 32),
    toFixedString('IFXGIIDBEB3WS3TF', 16),
    toFixedString('IFXGIIDBNZXXI2DFOIQHO2LOMU======', 32),
    toFixedString('IFXGIIDBEBWGK3LPNZQWIZI=', 24),
    toFixedString('OQYVU5RSPFQVU===', 16),
    toFixedString('IFXGIIDBNZXXI2DFOIQHO2LOMU======', 32)
    ]) val);

SELECT 'Part 7 - Similar to 02337_base58.sql';

SELECT base32Decode(encoded) FROM (SELECT base32Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase32Decode(encoded) FROM (SELECT base32Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase32Decode(val) FROM (SELECT arrayJoin(['Hold my beer', 'Hold another beer', '3csAg9', 'And a wine', 'And another wine', 'And a lemonade', 't1Zv2yaZ', 'And another wine']) val);

SELECT base32Encode(val) FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base32Decode(val) FROM (SELECT arrayJoin(['', 'MY======', 'MZXQ====', 'MZXW6===', 'MZXW6YQ=', 'MZXW6YTB', 'MZXW6YTBOI======']) val);

SELECT base32Encode(base32Decode('KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO===')) == 'KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO===';
SELECT base32Encode('\xAB\xCD\xEF\x01\x23') == 'VPG66AJD';

SELECT base32Encode(toFixedString('Hold my beer...', 15));
SELECT base32Decode(toFixedString('t1Zv2yaZ', 8)); -- { serverError INCORRECT_DATA }
SELECT tryBase32Decode(toFixedString('t1Zv2yaZ', 8));

SELECT base32Encode(val) FROM (SELECT arrayJoin([toFixedString('', 3), toFixedString('f', 3), toFixedString('fo', 3), toFixedString('foo', 3)]) val);
SELECT base32Decode(val) FROM (SELECT arrayJoin([toFixedString('AAAAA===', 8), toFixedString('MYAAA===', 8), toFixedString('MZXQA===', 8), toFixedString('MZXW6===', 8)]) val);

SELECT base32Encode(reinterpretAsFixedString(byteSwap(toUInt256('256')))) == 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAA====';
SELECT base32Encode(reinterpretAsString(byteSwap(toUInt256('256')))) == 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE======';  -- { reinterpretAsString drops the last null byte hence, encoded value is different than the FixedString version above }

SELECT base32Encode('Testing') == 'KRSXG5DJNZTQ====';
SELECT base32Decode('KRSXG5DJNZTQ====') == 'Testing';

SELECT base32Encode(val) FROM (SELECT arrayJoin(['test1', 'test2', 'test3', 'test123', 'test456']) val);
SELECT base32Decode(val) FROM (SELECT arrayJoin(['KRSXG5A=', 'ORSXG5BA', 'ORSXG5BB']) val);

DROP TABLE IF EXISTS t3447;
