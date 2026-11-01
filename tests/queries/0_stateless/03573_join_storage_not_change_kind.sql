DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS events;

CREATE TABLE users (uid UInt64, name String, age UInt8) Engine = Join(ALL, LEFT, uid);
CREATE TABLE users_n (uid UInt64, name String, age UInt8) Engine = Join(ALL, LEFT, uid) SETTINGS join_use_nulls = 1;

CREATE TABLE events (uid UInt64, user_id UInt64, message String) Engine = Memory;

INSERT INTO users VALUES (1, 'John', 33), (2, 'Ksenia', 48), (3, 'Alice', 50) ;
INSERT INTO users_n VALUES (1, 'John', 33), (2, 'Ksenia', 48), (3, 'Alice', 50) ;

INSERT INTO events VALUES (1, 1, 'hello'), (2, 2, 'hello world!') ;

SELECT event.message, user.name, user.age
FROM events event
LEFT JOIN users user ON event.user_id = user.uid
WHERE user.uid > 0
ORDER BY user.age
;

SET join_use_nulls = 1;

SELECT event.message, user.name, user.age
FROM events event
LEFT JOIN users_n user ON event.user_id = user.uid
WHERE user.uid > 0
ORDER BY user.age
;
