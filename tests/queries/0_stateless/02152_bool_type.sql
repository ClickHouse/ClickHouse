SELECT CAST('True', 'Bool');
SELECT CAST('TrUe', 'Bool');
SELECT CAST('true', 'Bool');
SELECT CAST('On', 'Bool');
SELECT CAST('on', 'Bool');
SELECT CAST('Yes', 'Bool');
SELECT CAST('yes', 'Bool');
SELECT CAST('T', 'Bool');
SELECT CAST('t', 'Bool');
SELECT CAST('Y', 'Bool');
SELECT CAST('y', 'Bool');
SELECT CAST('1', 'Bool');
SELECT CAST('enabled', 'Bool');
SELECT CAST('enable', 'Bool');

SELECT CAST('False', 'Bool');
SELECT CAST('FaLse', 'Bool');
SELECT CAST('false', 'Bool');
SELECT CAST('Off', 'Bool');
SELECT CAST('off', 'Bool');
SELECT CAST('No', 'Bool');
SELECT CAST('no', 'Bool');
SELECT CAST('N', 'Bool');
SELECT CAST('n', 'Bool');
SELECT CAST('F', 'Bool');
SELECT CAST('f', 'Bool');
SELECT CAST('0', 'Bool');
SELECT CAST('disabled', 'Bool');
SELECT CAST('disable', 'Bool');

SET bool_true_representation = 'Custom true';
SET bool_false_representation = 'Custom false';

SELECT CAST('true', 'Bool') format CSV;
SELECT CAST('true', 'Bool') format TSV;
SELECT CAST('true', 'Bool') format Values;
SELECT '';
SELECT CAST('true', 'Bool') format Vertical;
SELECT CAST('true', 'Bool') format Pretty;
SELECT CAST('true', 'Bool') format JSONEachRow;

SELECT CAST(CAST(2, 'Bool'), 'UInt8');
SELECT CAST(CAST(toUInt32(2), 'Bool'), 'UInt8');
SELECT CAST(CAST(toInt8(2), 'Bool'), 'UInt8');
SELECT CAST(CAST(toFloat32(2), 'Bool'), 'UInt8');
SELECT CAST(CAST(toDecimal32(2, 2), 'Bool'), 'UInt8');
SELECT CAST(CAST(materialize(2), 'Bool'), 'UInt8');

