SELECT 'привет пр' AS x, lengthUTF8(x) AS y;
SELECT x, lengthUTF8(x) AS y FROM (SELECT arrayJoin(['', 'h', 'hello', 'hello hello hello', 'п', 'пр', 'привет', 'привет привет', 'привет привет привет', '你好', '你好 你好', '你好你好你好', '你好你好你好你好', '你好 你好 你好 你好 你好']) AS x);
