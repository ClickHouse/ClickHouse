set enable_analyzer=1;

SELECT count(), plus((-9, 0), (number,  number)) AS k FROM remote('127.0.0.{3,2}', numbers(2)) GROUP BY k;
