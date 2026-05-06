-- Tags: no-fasttest

SELECT hex(keccak256(''));
SELECT hex(keccak256(unhex('')));
SELECT hex(keccak256('hello'));
SELECT hex(keccak256(toFixedString('hello', 5)));
SELECT hex(keccak256(toFixedString('hello', 6)));
SELECT hex(keccak256('Hello'));
SELECT hex(keccak256('Ethereum'));
SELECT hex(keccak256(repeat('a', 1000)));
SELECT hex(keccak256('Hello, 世界!'));
SELECT keccak256(NULL) AS null_hash;
SELECT hex(keccak256(unhex('deadbeef'))) AS binary_hash;
SELECT hex(keccak256('\n\t\r')) AS escaped_chars_hash, hex(keccak256('\\n\\t\\r')) AS literal_backslash_hash;
SELECT hex(keccak256(keccak256('test'))) AS double_hash;

WITH iterations AS ( SELECT number AS n FROM system.numbers LIMIT 5 )
SELECT hex(keccak256('consistent')), hex(keccak256(toString(n))) FROM iterations
ORDER BY n;
