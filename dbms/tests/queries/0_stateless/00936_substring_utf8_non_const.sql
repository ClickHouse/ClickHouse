SELECT substringUTF8('hello, привет', 1, number) FROM numbers(16);
SELECT substringUTF8('hello, привет', number + 1, 3) FROM numbers(16);
SELECT substringUTF8('hello, привет', number + 1, number) FROM numbers(16);
SELECT substringUTF8('hello, привет', -1 - number, 5) FROM numbers(16);
SELECT substringUTF8('hello, привет', -1 - number) FROM numbers(16);
SELECT substringUTF8('hello, привет', 1 + number) FROM numbers(16);

SELECT substringUTF8('hello, привет', 1) FROM numbers(3);
SELECT substringUTF8('hello, привет', 5) FROM numbers(3);
SELECT substringUTF8('hello, привет', 1, 10) FROM numbers(3);
SELECT substringUTF8('hello, привет', 5, 5) FROM numbers(3);
SELECT substringUTF8('hello, привет', -5) FROM numbers(3);
SELECT substringUTF8('hello, привет', -10, 5) FROM numbers(3);

SELECT substringUTF8(materialize('hello, привет'), 1, number) FROM numbers(16);
SELECT substringUTF8(materialize('hello, привет'), number + 1, 3) FROM numbers(16);
SELECT substringUTF8(materialize('hello, привет'), number + 1, number) FROM numbers(16);
SELECT substringUTF8(materialize('hello, привет'), -1 - number, 5) FROM numbers(16);
SELECT substringUTF8(materialize('hello, привет'), -1 - number) FROM numbers(16);
SELECT substringUTF8(materialize('hello, привет'), 1 + number) FROM numbers(16);

SELECT substringUTF8(materialize('hello, привет'), 1) FROM numbers(3);
SELECT substringUTF8(materialize('hello, привет'), 5) FROM numbers(3);
SELECT substringUTF8(materialize('hello, привет'), 1, 10) FROM numbers(3);
SELECT substringUTF8(materialize('hello, привет'), 5, 5) FROM numbers(3);
SELECT substringUTF8(materialize('hello, привет'), -5) FROM numbers(3);
SELECT substringUTF8(materialize('hello, привет'), -10, 5) FROM numbers(3);
