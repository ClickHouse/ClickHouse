DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table3;

CREATE TABLE table2 AS numbers(5);
CREATE TABLE table3 AS table2;

SHOW CREATE table2;
SHOW CREATE table3;

SELECT count(), sum(number) FROM table2;
SELECT count(), sum(number) FROM table3;

DROP TABLE table2;
DROP TABLE table3;
