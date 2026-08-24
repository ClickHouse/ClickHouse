SELECT DISTINCT toString(number) = toStringCutToZero(toString(number)) FROM (SELECT * FROM system.numbers LIMIT 1000);
SELECT DISTINCT toString(number) = toStringCutToZero(toFixedString(toString(number), 10)) FROM (SELECT * FROM system.numbers LIMIT 1000);
