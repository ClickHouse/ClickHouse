CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT count(*) OVER w 
FROM users WINDOW w AS (ORDER BY uid), w AS(ORDER BY name); -- { serverError BAD_ARGUMENTS }
