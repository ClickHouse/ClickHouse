-- Tags: no-fasttest, no-openssl-fips

SELECT hex(halfMD5('test'));
SELECT hex(MD4('test'));
SELECT hex(MD5('test'));
SELECT hex(SHA1('test'));
SELECT hex(SHA224('test'));
SELECT hex(SHA256('test'));
SELECT hex(SHA384('test'));
SELECT hex(SHA512('test'));
SELECT hex(SHA512_256('test'));

SELECT length(s), hex(MD5(s))
FROM
(
    SELECT arrayJoin([
        '',
        'a',
        'abc',
        'message digest',
        'abcdefghijklmnopqrstuvwxyz',
        repeat('x', 55),
        repeat('x', 56),
        repeat('x', 63),
        repeat('x', 64),
        repeat('x', 65),
        repeat('x', 127),
        repeat('x', 128),
        repeat('x', 129),
        repeat('x', 1000)]) AS s
);

SELECT number, length(s), hex(MD5(s))
FROM
(
    SELECT number, repeat(toString(number), (number % 10) * 13 + 1) AS s FROM numbers(24)
);

SELECT number, hex(MD5(toString(number))) FROM numbers(16);

SELECT number, hex(MD5(toFixedString(toString(number), 4))) FROM numbers(16);

SELECT number, hex(MD5(toIPv6(concat('2001:db8::', toString(number))))) FROM numbers(16);
