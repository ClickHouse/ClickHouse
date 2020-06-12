WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'x' AS a, prefix || 'y' AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);
WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'y' AS a, prefix || 'x' AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);
WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'x' AS a, prefix || 'x' AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);

WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'x' || prefix AS a, prefix || 'y' || prefix AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);
WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'y' || prefix AS a, prefix || 'x' || prefix AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);
WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'x' || prefix AS a, prefix || 'x' || prefix AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);

WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'x' || prefix AS a, prefix || 'y' AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);
WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'y' || prefix AS a, prefix || 'x' AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);
WITH substring('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, number) AS prefix, prefix || 'x' || prefix AS a, prefix || 'x' AS b SELECT a = b, a < b, a > b, a <= b, a >= b FROM numbers(40);

WITH arrayJoin(['aaa', 'bbb']) AS a, 'aaa\0bbb' AS b SELECT a = b, a < b, a > b, a <= b, a >= b;
WITH arrayJoin(['aaa', 'zzz']) AS a, 'aaa\0bbb' AS b SELECT a = b, a < b, a > b, a <= b, a >= b;
WITH arrayJoin(['aaa', 'bbb']) AS a, materialize('aaa\0bbb') AS b SELECT a = b, a < b, a > b, a <= b, a >= b;
WITH arrayJoin(['aaa', 'zzz']) AS a, materialize('aaa\0bbb') AS b SELECT a = b, a < b, a > b, a <= b, a >= b;

SELECT empty(toFixedString('', 1 + randConstant() % 100));
