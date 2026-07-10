SELECT conv(toFixedString('10', 5), 10, 2);
SELECT conv(toFixedString(toString(number), 4), 10, 16) FROM numbers(3) ORDER BY number ASC;
