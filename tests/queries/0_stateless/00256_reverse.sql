SELECT reverse('Hello');
SELECT reverse(materialize('Hello'));
SELECT reverse(toString(round(exp10(number)))) FROM system.numbers LIMIT 10;

SELECT reverse(['Hello', 'World']);
SELECT reverse(materialize(['Hello', 'World']));
SELECT reverse(range(number)) FROM system.numbers LIMIT 10;
SELECT reverse(arrayMap(x -> toString(round(exp10(x))), range(number))) FROM system.numbers LIMIT 10;
SELECT reverse(toFixedString(toString(round(exp10(number))), 10)) FROM system.numbers LIMIT 10;
