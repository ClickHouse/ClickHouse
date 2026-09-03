SET optimize_multiif_to_if = 0;

SELECT if(number % 2, toFixedString(toString(number), 2), toFixedString(toString(-number), 5)) AS x, toTypeName(x) FROM system.numbers LIMIT 10;
SELECT if(number % 2, toFixedString(toString(number), 2), toFixedString(toString(-number), 2)) AS x, toTypeName(x) FROM system.numbers LIMIT 10;

SELECT multiIf(number % 2, toFixedString(toString(number), 2), toFixedString(toString(-number), 5)) AS x, toTypeName(x) FROM system.numbers LIMIT 10;
SELECT multiIf(number % 2, toFixedString(toString(number), 2), toFixedString(toString(-number), 2)) AS x, toTypeName(x) FROM system.numbers LIMIT 10;
