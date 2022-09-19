-- Tags: no-backward-compatibility-check

SELECT toDate('2022-02-01') AS d1
FROM numbers(18) AS number
ORDER BY d1 ASC WITH FILL FROM toDateTime('2022-02-01') TO toDateTime('2022-07-01') STEP toIntervalMonth(1); -- { serverError 475 }

