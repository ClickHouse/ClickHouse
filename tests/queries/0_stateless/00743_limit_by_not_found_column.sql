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
);

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

CREATE TEMPORARY TABLE commententry1 (created_date Date, link_id String, subreddit String);
INSERT INTO commententry1 VALUES ('2016-01-01', 'xyz', 'cpp');

SELECT concat('http://reddit.com/r/', subreddit, '/comments/', replaceRegexpOne(link_id, 't[0-9]_', ''))
FROM
(
    SELECT
        y,
        subreddit,
        link_id,
        cnt
    FROM
    (
        SELECT
            created_date AS y,
            link_id,
            subreddit,
            count(*) AS cnt
        FROM commententry1
        WHERE toYear(created_date) = 2016
        GROUP BY
            y,
            link_id,
            subreddit
        ORDER BY y ASC
    )
    ORDER BY
        y ASC,
        cnt DESC
    LIMIT 1 BY y
);
