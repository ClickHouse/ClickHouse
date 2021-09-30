-- { echo }
SELECT '' FROM numbers(1) GROUP BY number WITH TOTALS HAVING count(number)>0;
SELECT '' FROM numbers(1) GROUP BY number WITH TOTALS HAVING count(number)<0;
