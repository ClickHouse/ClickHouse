-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

SELECT hex(lowerUTF8('\xFF'));
SELECT hex(upperUTF8('\xFF'));
