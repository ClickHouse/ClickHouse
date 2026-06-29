-- Tags: stateful
SELECT UserID, arrayEnumerateUniq(groupArray(SearchPhrase)) AS arr
FROM
(
    SELECT UserID, SearchPhrase
    FROM test.hits
    WHERE CounterID = 1704509 AND UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE notEmpty(SearchPhrase) AND CounterID = 1704509
        GROUP BY UserID
        HAVING count() > 1
    )
    ORDER BY UserID, WatchID
)
WHERE notEmpty(SearchPhrase)
GROUP BY UserID
HAVING length(arr) > 1
ORDER BY UserID
LIMIT 20
