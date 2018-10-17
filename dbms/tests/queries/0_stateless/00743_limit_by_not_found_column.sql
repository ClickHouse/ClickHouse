USE test;
DROP TABLE IF EXISTS installation_stats;
CREATE TABLE installation_stats (message String, info String, message_type String) ENGINE = Log;

SELECT count(*) AS total
FROM
(
    SELECT
        message,
        info,
        count() AS cnt
    FROM installation_stats
    WHERE message_type LIKE 'fail'
    GROUP BY
        message,
        info
    ORDER BY cnt DESC
    LIMIT 5 BY message
)

DROP TABLE installation_stats;

CREATE TEMPORARY TABLE Accounts (AccountID UInt64, Currency String);

SELECT AccountID
FROM 
(
    SELECT 
        AccountID, 
        Currency
    FROM Accounts 
    LIMIT 2 BY Currency
);
