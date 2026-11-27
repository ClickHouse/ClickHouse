-- A constant needle that matches everything, against a FixedString haystack column.
-- Two rows: the answer is filled per row, so a write to the first row alone must be visible.
SELECT s, s LIKE '%', s NOT LIKE '%', s ILIKE '%', match(s, '.*')
FROM (SELECT toFixedString(arrayJoin(['aa', 'bb']), 2) AS s)
ORDER BY s;
