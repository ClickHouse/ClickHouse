SELECT DISTINCT bitXor(materialize(toFixedString('abc', 3)), toFixedString('\x00\x01\x02', 3)) FROM numbers(10);
SELECT DISTINCT bitXor(materialize(toFixedString('abcdef', 6)), toFixedString('\x00\x01\x02\x03\x04\x05', 6)) FROM numbers(10);

SELECT DISTINCT bitXor(toFixedString('\x00\x01\x02', 3), materialize(toFixedString('abc', 3))) FROM numbers(10);
SELECT DISTINCT bitXor(toFixedString('\x00\x01\x02\x03\x04\x05', 6), materialize(toFixedString('abcdef', 6))) FROM numbers(10);
