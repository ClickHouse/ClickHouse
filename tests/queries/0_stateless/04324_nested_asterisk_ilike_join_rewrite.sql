SELECT tuple(a.* ILIKE '%id')
FROM (SELECT 1 AS UserID, 10 AS left_value) AS a
LEFT JOIN (SELECT 1 AS id, 20 AS right_value) AS b ON b.id = a.UserID
LEFT JOIN (SELECT 1 AS id, 30 AS other_value) AS c ON c.id = a.UserID
SETTINGS enable_analyzer = 1;

SELECT tuple(a.* ILIKE '%id')
FROM (SELECT 1 AS UserID, 10 AS a) AS a
LEFT JOIN (SELECT 1 AS id, 20 AS a) AS b ON b.id = a.UserID
LEFT JOIN (SELECT 1 AS id, 30 AS a) AS c ON c.id = a.UserID
SETTINGS enable_analyzer = 0;

SELECT tuple(* ILIKE '%value')
FROM (SELECT 1 AS id, 'left' AS left_value) AS a
LEFT JOIN (SELECT 1 AS id, 'right' AS right_value) AS b ON b.id = a.id
LEFT JOIN (SELECT 1 AS id, 'other' AS other_value) AS c ON c.id = a.id
SETTINGS enable_analyzer = 1;

SELECT tuple(* ILIKE '%value')
FROM (SELECT 1 AS id, 'left' AS left_value) AS a
LEFT JOIN (SELECT 1 AS id, 'right' AS right_value) AS b ON b.id = a.id
LEFT JOIN (SELECT 1 AS id, 'other' AS other_value) AS c ON c.id = a.id
SETTINGS enable_analyzer = 0;

SELECT (a.* ILIKE '%id') + 1
FROM (SELECT 1 AS UserID, 10 AS a) AS a
LEFT JOIN (SELECT 1 AS id, 20 AS a) AS b ON b.id = a.UserID
LEFT JOIN (SELECT 1 AS id, 30 AS a) AS c ON c.id = a.UserID
SETTINGS enable_analyzer = 0;

-- The matcher expands to two arguments here, so binary `plus` receives too many arguments.
SELECT (a.* ILIKE '%id') + 1
FROM (SELECT 1 AS UserID, 2 AS session_id, 10 AS a) AS a
LEFT JOIN (SELECT 1 AS id, 20 AS a) AS b ON b.id = a.UserID
LEFT JOIN (SELECT 1 AS id, 30 AS a) AS c ON c.id = a.UserID
SETTINGS enable_analyzer = 0; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT count(*)
FROM (SELECT 1 AS UserID, 10 AS a) AS a
LEFT JOIN (SELECT 1 AS id, 20 AS a) AS b ON b.id = a.UserID
LEFT JOIN (SELECT 1 AS id, 30 AS a) AS c ON c.id = a.UserID
SETTINGS enable_analyzer = 0;
