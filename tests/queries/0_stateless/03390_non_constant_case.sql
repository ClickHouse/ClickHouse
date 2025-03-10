SELECT 'Numeric CASE';
SELECT number,
    CASE number % 2
        WHEN number % 3 THEN 'Match Mod 3'
        WHEN number % 4 THEN 'Match Mod 4'
        ELSE 'No Match'
    END AS result
FROM numbers(10);

SELECT 'String CASE';
SELECT name,
    CASE name
        WHEN substring(name, 1, 1) THEN 'First letter'
        WHEN reverse(name) THEN 'Reversed match'
        ELSE 'No Match'
    END AS result
FROM (SELECT arrayJoin(['Alice', 'Bob', 'Charlie', 'David', 'abba', 'A']) AS name);

SELECT 'Date CASE';
SELECT event_date,
    CASE event_date
        WHEN toDate('2024-03-10') THEN 'Special day'
        WHEN today() THEN 'Today'
        ELSE 'Normal Day'
    END AS event_type
FROM (SELECT arrayJoin([toDate('2024-03-10'), toDate('2024-03-11'), today()]) AS event_date);

SELECT '1M Rows';
SELECT count() FROM numbers(1000000) WHERE CASE number % 2
    WHEN number % 3 THEN 1
    WHEN number % 5 THEN 1
    ELSE 0
END = 1;
