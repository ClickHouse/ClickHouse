-- Tags: no-fasttest
-- no-fasttest: requires idna library

-- Behaviour pinned after bumping `idna` to 0.5.5.
-- See also 02932_idna.sql and 02932_punycode.sql.

SELECT '-- ToUnicode leaves invalid xn-- labels untouched instead of emitting replacement characters';
SELECT idnaDecode('xn--zn7c.com');
SELECT idnaDecode('xn--hdhxn--');
SELECT idnaDecode('a.b.c.xn--pokxncvks');
SELECT idnaDecode('xn--a.xn--zca');

SELECT '-- punycodeDecode rejects invalid input instead of returning garbage';
SELECT tryPunycodeDecode('xn--zca.xn--zca');
SELECT tryPunycodeDecode('xn----xhn');
SELECT punycodeDecode('xn--zca.xn--zca'); -- { serverError BAD_ARGUMENTS }

SELECT '-- The bidi rule rejects labels which mix Arabic-Indic digits with strong LTR characters';
SELECT tryIdnaEncode('1ا');
SELECT tryIdnaEncode('٠ا');
SELECT tryIdnaEncode('١٢٣.com');
SELECT idnaEncode('1ا'); -- { serverError BAD_ARGUMENTS }
