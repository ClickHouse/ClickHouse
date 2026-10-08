SELECT CASE number
    WHEN number * 2 - 4 THEN 'Hello'
    WHEN number * 3 - 6 THEN 'world'
    ELSE '' END
FROM numbers(10);
