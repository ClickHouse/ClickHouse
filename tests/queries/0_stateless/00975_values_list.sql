DROP TABLE IF EXISTS values_list;

SELECT * FROM VALUES('a UInt64, s String', (1, 'one'), (2, 'two'), (3, 'three'));
CREATE TABLE values_list AS VALUES('a UInt64, s String', (1, 'one'), (2, 'two'), (3, 'three'));
SELECT * FROM values_list;

SELECT subtractYears(date, 1), subtractYears(date_time, 1) FROM VALUES('date Date, date_time DateTime', (toDate('2019-01-01'), toDateTime('2019-01-01 00:00:00')));

SELECT * FROM VALUES('s String', ('abra'), ('cadabra'), ('abracadabra'));

SELECT * FROM VALUES('n UInt64, s String, ss String', (1 + 22, '23', toString(23)), (toUInt64('24'), '24', concat('2', '4')));

SELECT * FROM VALUES('a Decimal(4, 4), b String, c String', (divide(toDecimal32(5, 3), 3), 'a', 'b'));

SELECT * FROM VALUES('x Float64', toUInt64(-1)); -- { serverError 69; }
SELECT * FROM VALUES('x Float64', NULL); -- { serverError 53; }
SELECT * FROM VALUES('x Nullable(Float64)', NULL);

DROP TABLE values_list;
