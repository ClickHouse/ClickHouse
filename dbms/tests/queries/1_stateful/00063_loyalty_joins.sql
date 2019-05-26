SELECT 
    loyalty, 
    count()
FROM test.hits ANY LEFT JOIN 
(
    SELECT 
        UserID, 
        sum(SearchEngineID = 2) AS yandex, 
        sum(SearchEngineID = 3) AS google, 
        toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM test.hits
    WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
    GROUP BY UserID
    HAVING (yandex + google) > 10
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT 
    loyalty, 
    count()
FROM 
(
    SELECT UserID
    FROM test.hits
) ANY LEFT JOIN 
(
    SELECT 
        UserID, 
        sum(SearchEngineID = 2) AS yandex, 
        sum(SearchEngineID = 3) AS google, 
        toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM test.hits
    WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
    GROUP BY UserID
    HAVING (yandex + google) > 10
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT 
    loyalty, 
    count()
FROM 
(
    SELECT 
        loyalty, 
        UserID
    FROM 
    (
        SELECT UserID
        FROM test.hits
    ) ANY LEFT JOIN 
    (
        SELECT 
            UserID, 
            sum(SearchEngineID = 2) AS yandex, 
            sum(SearchEngineID = 3) AS google, 
            toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
        FROM test.hits
        WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
        GROUP BY UserID
        HAVING (yandex + google) > 10
    ) USING UserID
)
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT 
    loyalty, 
    count() AS c, 
    bar(log(c + 1) * 1000, 0, log(3000000) * 1000, 80)
FROM test.hits ANY INNER JOIN 
(
    SELECT 
        UserID, 
        toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM 
    (
        SELECT 
            UserID, 
            sum(SearchEngineID = 2) AS yandex, 
            sum(SearchEngineID = 3) AS google
        FROM test.hits
        WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
        GROUP BY UserID
        HAVING (yandex + google) > 10
    )
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;
