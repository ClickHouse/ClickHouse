-- Tags: no-fasttest

SELECT base58Encode('Hold my beer...');

SELECT base58Encode('Hold my beer...', 'Second arg'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base58Decode('Hold my beer...'); -- { serverError BAD_ARGUMENTS }

SELECT base58Decode(encoded) FROM (SELECT base58Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase58Decode(encoded) FROM (SELECT base58Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase58Decode(val) FROM (SELECT arrayJoin(['Hold my beer', 'Hold another beer', '3csAg9', 'And a wine', 'And another wine', 'And a lemonade', 't1Zv2yaZ', 'And another wine']) val);

SELECT base58Encode(val) FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base58Decode(val) FROM (SELECT arrayJoin(['', '2m', '8o8', 'bQbp', '3csAg9', 'CZJRhmz', 't1Zv2yaZ', '']) val);

SELECT base58Encode(base58Decode('1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix')) == '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';
select base58Encode('\x00\x0b\xe3\xe1\xeb\xa1\x7a\x47\x3f\x89\xb0\xf7\xe8\xe2\x49\x40\xf2\x0a\xeb\x8e\xbc\xa7\x1a\x88\xfd\xe9\x5d\x4b\x83\xb7\x1a\x09') == '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';

SELECT base58Encode(toFixedString('Hold my beer...', 15));
SELECT base58Decode(toFixedString('t1Zv2yaZ', 8));

SELECT base58Encode(val) FROM (SELECT arrayJoin([toFixedString('', 3), toFixedString('f', 3), toFixedString('fo', 3), toFixedString('foo', 3)]) val);
SELECT base58Decode(val) FROM (SELECT arrayJoin([toFixedString('111', 3), toFixedString('bG7y', 4), toFixedString('bQZu', 4), toFixedString('bQbp', 4)]) val);

Select base58Encode(reinterpretAsFixedString(byteSwap(toUInt256('256')))) == '1111111111111111111111111111115R';
Select base58Encode(reinterpretAsString(byteSwap(toUInt256('256')))) == '1111111111111111111111111111112'; -- { reinterpretAsString drops the last null byte hence, encoded value is different than the FixedString version above }
