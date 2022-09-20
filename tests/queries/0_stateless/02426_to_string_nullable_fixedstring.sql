SELECT CAST('a', 'Nullable(FixedString(1))') as s,  toTypeName(s), toString(s);
SELECT toTypeName(s), toString(s) FROM (SELECT if(number % 3 = 0, NULL, toFixedString(toString(number), 1)) AS s from numbers(10))
