SELECT stringBytesUniq('Hello');
SELECT stringBytesUniq('');
SELECT stringBytesUniq('aaaa');
SELECT stringBytesUniq('abcABC123');
SELECT stringBytesUniq(toNullable('Hello'));
SELECT stringBytesUniq(toLowCardinality('Hello'));

SELECT stringBytesEntropy('Hello');
SELECT stringBytesEntropy('');
SELECT stringBytesEntropy('aaaa');
SELECT stringBytesEntropy('abcABC123');
SELECT stringBytesEntropy(toNullable('Hello'));
SELECT stringBytesEntropy(toLowCardinality('Hello'));

SELECT stringBytesUniq(NULL);
SELECT stringBytesEntropy(NULL);
SELECT stringBytesUniq(toNullable(NULL));
SELECT stringBytesEntropy(toNullable(NULL));
