SELECT dummy, count() / 0.1 GROUP BY dummy WITH TOTALS HAVING count() > 0.1
