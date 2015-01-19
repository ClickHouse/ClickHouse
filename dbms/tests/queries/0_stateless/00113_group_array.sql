SELECT intDiv(number, 100) AS k, length(groupArray(number)) FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;
