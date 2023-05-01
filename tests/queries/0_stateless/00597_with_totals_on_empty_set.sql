SELECT count() FROM numbers(10) WHERE number = -1 WITH TOTALS FORMAT Vertical;
SELECT count() FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
SELECT number, count() FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
SELECT groupArray(number) FROM numbers(10) WHERE number = -1 WITH TOTALS FORMAT Vertical;
SELECT groupArray(number) FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
SELECT number, groupArray(number) FROM numbers(10) WHERE number = -1 GROUP BY number WITH TOTALS FORMAT Vertical;
