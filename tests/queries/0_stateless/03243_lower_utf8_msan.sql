-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

SELECT ignore(lengthUTF8(lowerUTF8(randomStringUTF8(99)))); -- bug #49672: msan assert
