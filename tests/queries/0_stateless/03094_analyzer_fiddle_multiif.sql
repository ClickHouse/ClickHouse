DROP TABLE IF EXISTS users_03094;

CREATE TABLE users_03094 (name String, age Int16) ENGINE=Memory;
INSERT INTO users_03094 VALUES ('John', 33);
INSERT INTO users_03094 VALUES ('Ksenia', 48);
INSERT INTO users_03094 VALUES ('Alice', 50);

SET enable_analyzer=1;

SELECT
    multiIf((age > 30) or (true), '1', '2') AS a,
    max(name)
FROM users_03094
GROUP BY a;

DROP TABLE IF EXISTS users_03094;
