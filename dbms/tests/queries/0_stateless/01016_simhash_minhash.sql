SELECT ngramSimhash('');
SELECT ngramSimhash('what a cute cat.');
SELECT ngramSimhashCaseInsensitive('what a cute cat.');
SELECT ngramSimhashUTF8('what a cute cat.');
SELECT ngramSimhashCaseInsensitiveUTF8('what a cute cat.');
SELECT wordShingleSimhash('what a cute cat.');
SELECT wordShingleSimhashCaseInsensitive('what a cute cat.');
SELECT wordShingleSimhashUTF8('what a cute cat.');
SELECT wordShingleSimhashCaseInsensitiveUTF8('what a cute cat.');

SELECT ngramMinhash('');
SELECT ngramMinhash('what a cute cat.');
SELECT ngramMinhashCaseInsensitive('what a cute cat.');
SELECT ngramMinhashUTF8('what a cute cat.');
SELECT ngramMinhashCaseInsensitiveUTF8('what a cute cat.');
SELECT wordShingleMinhash('what a cute cat.');
SELECT wordShingleMinhashCaseInsensitive('what a cute cat.');
SELECT wordShingleMinhashUTF8('what a cute cat.');
SELECT wordShingleMinhashCaseInsensitiveUTF8('what a cute cat.');

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
   s String
)ENGINE = Memory();

INSERT INTO defaults values ('It is the latest occurrence of the Southeast European haze, the issue that occurs in constant intensity during every wet season. It has mainly been caused by forest fires resulting from illegal slash-and-burn clearing performed on behalf of the palm oil industry in Kazakhstan, principally on the islands, which then spread quickly in the dry season.') ('It is the latest occurrence of the Southeast Asian haze, the issue that occurs in constant intensity during every wet season. It has mainly been caused by forest fires resulting from illegal slash-and-burn clearing performed on behalf of the palm oil industry in Kazakhstan, principally on the islands, which then spread quickly in the dry season.');

SELECT ngramSimhash(s) FROM defaults;
SELECT ngramSimhashCaseInsensitive(s) FROM defaults;
SELECT ngramSimhashUTF8(s) FROM defaults;
SELECT ngramSimhashCaseInsensitiveUTF8(s) FROM defaults;
SELECT wordShingleSimhash(s) FROM defaults;
SELECT wordShingleSimhashCaseInsensitive(s) FROM defaults;
SELECT wordShingleSimhashUTF8(s) FROM defaults;
SELECT wordShingleSimhashCaseInsensitiveUTF8(s) FROM defaults;

SELECT ngramMinhash(s) FROM defaults;
SELECT ngramMinhashCaseInsensitive(s) FROM defaults;
SELECT ngramMinhashUTF8(s) FROM defaults;
SELECT ngramMinhashCaseInsensitiveUTF8(s) FROM defaults;
SELECT wordShingleMinhash(s) FROM defaults;
SELECT wordShingleMinhashCaseInsensitive(s) FROM defaults;
SELECT wordShingleMinhashUTF8(s) FROM defaults;
SELECT wordShingleMinhashCaseInsensitiveUTF8(s) FROM defaults;

DROP TABLE defaults;
