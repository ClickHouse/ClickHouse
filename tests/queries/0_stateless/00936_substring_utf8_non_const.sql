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

SELECT DISTINCT substring(toString(range(rand(1) % 50)), rand(2) % 50, rand(3) % 50) = substringUTF8(toString(range(rand(1) % 50)), rand(2) % 50, rand(3) % 50) AS res FROM numbers(1000000);
SELECT DISTINCT substring(toString(range(rand(1) % 50)), rand(2) % 50) = substringUTF8(toString(range(rand(1) % 50)), rand(2) % 50) AS res FROM numbers(1000000);

-- NOTE: The behaviour of substring and substringUTF8 is inconsistent when negative offset is greater than string size:
-- substring:
--      hello
-- ^-----^ - offset -10, length 7, result: "he"
-- substringUTF8:
--      hello
--      ^-----^ - offset -10, length 7, result: "hello"

-- SELECT DISTINCT substring(toString(range(rand(1) % 50)), -(rand(2) % 50), rand(3) % 50) = substringUTF8(toString(range(rand(1) % 50)), -(rand(2) % 50), rand(3) % 50) AS res FROM numbers(1000000);

SELECT DISTINCT substring(toString(range(rand(1) % 50)), -(rand(2) % 50)) = substringUTF8(toString(range(rand(1) % 50)), -(rand(2) % 50)) AS res FROM numbers(1000000);
