SELECT tuple(a.* ILIKE '%id')
FROM (SELECT 1 AS UserID, 10 AS left_value) AS a
LEFT JOIN (SELECT 1 AS id, 20 AS right_value) AS b ON b.id = a.UserID
LEFT JOIN (SELECT 1 AS id, 30 AS other_value) AS c ON c.id = a.UserID
SETTINGS enable_analyzer = 1;


SELECT tuple(* ILIKE '%value')
FROM (SELECT 1 AS id, 'left' AS left_value) AS a
LEFT JOIN (SELECT 1 AS id, 'right' AS right_value) AS b ON b.id = a.id
LEFT JOIN (SELECT 1 AS id, 'other' AS other_value) AS c ON c.id = a.id
SETTINGS enable_analyzer = 1;



-- The matcher expands to two arguments here, so binary `plus` receives too many arguments.

