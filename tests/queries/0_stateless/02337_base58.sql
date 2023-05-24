-- Tags: no-fasttest

SET send_logs_level = 'fatal';

SELECT base58Encode('Hold my beer...');
SELECT base58Encode('Hold my beer...', 'Second arg'); -- { serverError 42 }
SELECT base58Decode('Hold my beer...'); -- { serverError 36 }

SELECT base58Decode(encoded) FROM (SELECT base58Encode(val) as encoded FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));

SELECT base58Encode(val) FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base58Decode(val) FROM (select arrayJoin(['', '2m', '8o8', 'bQbp', '3csAg9', 'CZJRhmz', 't1Zv2yaZ']) val);
