DROP TABLE IF EXISTS clicks;
DROP TABLE IF EXISTS transactions;

CREATE TABLE clicks (domain String) ENGINE = Memory;
CREATE TABLE transactions (domain String) ENGINE = Memory;

INSERT INTO clicks VALUES ('facebook.com'), ('meta.ua'), ('google.com');
INSERT INTO transactions VALUES ('facebook.com'), ('meta.ua'), ('baidu.com');


SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
FORMAT JSONEachRow;


SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
FORMAT JSONEachRow;


SELECT DISTINCT * FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10

UNION ALL

SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
);


SELECT DISTINCT total, domain FROM
(
SELECT
    sum(total_count) AS total, 
    sum(facebookHits) AS facebook,
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10

UNION ALL

SELECT
    sum(total_count) AS total, 
    max(facebookHits) AS facebook,
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
)
ORDER BY domain, total;


SELECT * FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js1
ALL FULL OUTER JOIN
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js2
USING (total, domain)
ORDER BY total, domain;


SELECT total FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js1
ALL FULL OUTER JOIN
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js2
USING (total, domain)
ORDER BY total, domain;


SELECT domain FROM
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
    UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js1
ALL FULL OUTER JOIN
(
SELECT
    sum(total_count) AS total, 
    domain
FROM
(
    SELECT
        COUNT(*) AS total_count, 
        SUM(if(domain = 'facebook.com', 1, 0)) AS facebookHits, 
        domain
    FROM clicks 
    GROUP BY domain
UNION ALL 
    SELECT
        COUNT(*) AS total_count, 
        toUInt64(0) AS facebookHits, 
        domain
    FROM transactions 
    GROUP BY domain
) 
GROUP BY domain
ORDER BY domain
LIMIT 10
) js2
USING (total, domain)
ORDER BY total, domain;


DROP TABLE clicks;
DROP TABLE transactions;
