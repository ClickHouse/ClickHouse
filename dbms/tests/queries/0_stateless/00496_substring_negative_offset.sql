SELECT substring('abc', number - 5) FROM system.numbers LIMIT 10;
SELECT substring(materialize('abc'), number - 5) FROM system.numbers LIMIT 10;
SELECT substring(toFixedString('abc', 3), number - 5) FROM system.numbers LIMIT 10;
SELECT substring(materialize(toFixedString('abc', 3)), number - 5) FROM system.numbers LIMIT 10;
