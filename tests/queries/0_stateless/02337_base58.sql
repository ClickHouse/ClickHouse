-- Tags: no-fasttest

SELECT base58Encode('Hold my beer...');
SELECT base58Encode('Hold my beer...', 'Second arg'); -- { serverError 42 }
SELECT base58Decode('Hold my beer...'); -- { serverError 36 }

SELECT base58Decode(encoded) FROM (SELECT base58Encode(val) as encoded FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));

SELECT base58Encode(val) FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base58Decode(val) FROM (select arrayJoin(['', '2m', '8o8', 'bQbp', '3csAg9', 'CZJRhmz', 't1Zv2yaZ', '']) val);

SELECT base58Encode(base58Decode('1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix')) == '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';
select base58Encode('\x00\x0b\xe3\xe1\xeb\xa1\x7a\x47\x3f\x89\xb0\xf7\xe8\xe2\x49\x40\xf2\x0a\xeb\x8e\xbc\xa7\x1a\x88\xfd\xe9\x5d\x4b\x83\xb7\x1a\x09') == '1BWutmTvYPwDtmw9abTkS4Ssr8no61spGAvW1X6NDix';
