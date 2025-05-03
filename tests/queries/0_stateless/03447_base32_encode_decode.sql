SELECT base32Encode('This is a test string');

SELECT base32Encode('This is a test string', 'Second arg'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT base32Decode(encoded) FROM (SELECT base32Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase32Decode(encoded) FROM (SELECT base32Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase32Decode(val) FROM (SELECT arrayJoin(['Hold my beer', 'Hold another beer', '3csAg9', 'And a wine', 'And another wine', 'And a lemonade', 't1Zv2yaZ', 'And another wine']) val);

SELECT base32Encode(val) FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base32Decode(val) FROM (SELECT arrayJoin(['', 'MY======', 'MZXQ====', 'MZXW6===', 'MZXW6YQ=', 'MZXW6YTB', 'MZXW6YTBOI======']) val);

SELECT base32Encode(base32Decode('KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO===')) == 'KRUGKIDROVUWG2ZAMJZG653OEBTG66BANJ2W24DTEBXXMZLSEB2GQZJANRQXU6JAMRXWO===';
SELECT base32Encode('\xAB\xCD\xEF\x01\x23') == 'VPG66AJD';

SELECT base32Encode(toFixedString('Hold my beer...', 15));
SELECT base32Decode(toFixedString('t1Zv2yaZ', 8)); -- { serverError BAD_ARGUMENTS }
SELECT tryBase32Decode(toFixedString('t1Zv2yaZ', 8));

SELECT base32Encode(val) FROM (SELECT arrayJoin([toFixedString('', 3), toFixedString('f', 3), toFixedString('fo', 3), toFixedString('foo', 3)]) val);
SELECT base32Decode(val) FROM (SELECT arrayJoin([toFixedString('AAAAA===', 8), toFixedString('MYAAA===', 8), toFixedString('MZXQA===', 8), toFixedString('MZXW6===', 8)]) val);

SELECT base32Encode(reinterpretAsFixedString(byteSwap(toUInt256('256')))) == 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAA====';
SELECT base32Encode(reinterpretAsString(byteSwap(toUInt256('256')))) == 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE======';  -- { reinterpretAsString drops the last null byte hence, encoded value is different than the FixedString version above }

SELECT base32Encode('Testing') == 'KRSXG5DJNZTQ====';
SELECT base32Decode('KRSXG5DJNZTQ====') == 'Testing';

SELECT base32Encode(val) FROM (SELECT arrayJoin(['test1', 'test2', 'test3', 'test123', 'test456']) val);
SELECT base32Decode(val) FROM (SELECT arrayJoin(['KRSXG5A=', 'ORSXG5BA', 'ORSXG5BB']) val);
