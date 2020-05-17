SELECT count() FROM system.numbers WHERE number != number;
SELECT count() FROM system.numbers WHERE number < number;
SELECT count() FROM system.numbers WHERE number > number;

SELECT count() FROM system.numbers WHERE NOT (number = number);
SELECT count() FROM system.numbers WHERE NOT (number <= number);
SELECT count() FROM system.numbers WHERE NOT (number >= number);

SELECT count() FROM system.numbers WHERE SHA256(toString(number)) != SHA256(toString(number));
SELECT count() FROM system.numbers WHERE SHA256(toString(number)) != SHA256(toString(number)) AND rand() > 10;
