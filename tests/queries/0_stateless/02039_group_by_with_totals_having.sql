-- { echo }
SELECT 'x' FROM numbers(2) GROUP BY number WITH TOTALS HAVING count(number)>0;
SELECT 'x' FROM numbers(2) GROUP BY number WITH TOTALS HAVING count(number)<0;
