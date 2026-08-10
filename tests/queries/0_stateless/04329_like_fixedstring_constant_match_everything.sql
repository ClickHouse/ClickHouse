-- A constant needle that matches everything, against a FixedString haystack column.
SELECT s LIKE '%', s NOT LIKE '%', s ILIKE '%', match(s, '.*')
FROM (SELECT toFixedString(materialize('aa'), 2) AS s);
