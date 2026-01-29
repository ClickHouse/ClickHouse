-- Tags: no-fasttest

SELECT HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'));

SELECT HEX(RIPEMD160('The quick brown fox jumps over the lazy cog'));

SELECT HEX(RIPEMD160(''));

SELECT HEX(RIPEMD160('A-very-long-string-that-should-be-hashed-using-ripemd160'));

SELECT HEX(RIPEMD160(toString(avg(number))) )
FROM (SELECT arrayJoin([1, 2, 3, 4, 5]) AS number);
