-- Tags: no-fasttest

SET send_logs_level = 'fatal';

SELECT base58Encode('Hold my beer...');
SELECT base58Encode('Hold my beer...', ''); -- { serverError 44 }
SELECT base58Encode('Hold my beer...', 'gmp', 'third'); -- { serverError 36 }

SELECT base58Decode(encoded, 'gmp') FROM (SELECT base58Encode(val, 'gmp') as encoded FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT base58Decode(encoded, 'ripple') FROM (SELECT base58Encode(val, 'ripple') as encoded FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT base58Decode(encoded, 'flickr') FROM (SELECT base58Encode(val, 'flickr') as encoded FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT base58Decode(encoded, 'bitcoin') FROM (SELECT base58Encode(val, 'bitcoin') as encoded FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));

SELECT base58Encode(val) FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base58Decode(val) FROM (select arrayJoin(['', '2m', '8o8', 'bQbp', '3csAg9', 'CZJRhmz', 't1Zv2yaZ']) val);

SELECT base58Decode('Why_not?'); -- { serverError 36 }
