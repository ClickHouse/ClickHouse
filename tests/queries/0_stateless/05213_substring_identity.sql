SELECT substring('abcdef', 1);
SELECT substringUTF8('Zażółć gęślą jaźń', 1);

SELECT number, substring(concat('value_', toString(number)), 1)
FROM numbers(3)
ORDER BY number;

SELECT number, substringUTF8(if(number % 2, 'żółć', 'hello'), 1)
FROM numbers(3)
ORDER BY number;

SELECT substring('abcdef', 2);
SELECT substringUTF8('héllo', 2);
SELECT substring('abcdef', 1, 3);
SELECT toTypeName(substring(toFixedString('abc', 3), 1));
SELECT toTypeName(substring(CAST('foo', 'Enum8(\'foo\' = 1)'), 1));

SELECT number, substring(
    if(number = 1, CAST(NULL, 'Nullable(String)'), CAST('hello', 'Nullable(String)')),
    1)
FROM numbers(3)
ORDER BY number;
