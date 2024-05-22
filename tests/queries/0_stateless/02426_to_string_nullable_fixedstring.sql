SELECT CAST('a', 'Nullable(FixedString(1))') as s,  toTypeName(s), toString(s);
SELECT number, toTypeName(s), toString(s) FROM (SELECT number, if(number % 3 = 0, NULL, toFixedString(toString(number), 1)) AS s from numbers(10)) ORDER BY number;
