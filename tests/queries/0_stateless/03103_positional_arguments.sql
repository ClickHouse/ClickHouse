-- https://github.com/ClickHouse/ClickHouse/issues/56466

SET enable_analyzer=1;

DROP TABLE IF EXISTS users;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

-- The query works when using a single SELECT *
SELECT *
FROM
(
    SELECT
        name,
        age
    FROM users
)
GROUP BY
    1,
    2
ORDER BY ALL;

-- It doesn't when the GROUP BY is nested deeper
SELECT *
FROM
(
	SELECT *
	FROM
	(
    	SELECT
        	name,
        	age
    	FROM users
	)
	GROUP BY
    	1,
    	2
)
ORDER BY ALL;

DROP TABLE IF EXISTS users;
