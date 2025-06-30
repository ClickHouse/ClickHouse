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
        WHEN toDate('2024-03-12') THEN 'Today'
        ELSE 'Normal Day'
    END AS event_type
FROM (SELECT arrayJoin([toDate('2024-03-10'), toDate('2024-03-11'), toDate('2024-03-12')]) AS event_date);

SELECT '1M Rows';
SELECT count() FROM numbers(1000000) WHERE CASE number % 2
    WHEN number % 3 THEN 1
    WHEN number % 5 THEN 1
    ELSE 0
END = 1;

SELECT DISTINCT caseWithExpression(1.1, toNullable(0.1), 'a', 1.1, 'b', materialize(2.1), toFixedString('c', 1), 'default' ) AS f;

SELECT
    caseWithExpression(NULL, materialize(NULL), NULL, NULL) AS f1,
    if(NULL, toDateTimeOrZero(NULL), NULL) AS f2
FROM numbers(1);

SELECT CASE number WHEN 1 THEN number + 2 ELSE number * 2 END FROM numbers(3);

SELECT   caseWithExpression(
    materialize(
        materialize(NULL)
    ),
    materialize(NULL),
    NULL,
    NULL
);

SELECT caseWithExpression(
    materialize(
        assumeNotNull(
            materialize(NULL)
        )
    ),
    materialize(NULL),
    NULL,
    NULL
); -- { serverError ILLEGAL_COLUMN }
