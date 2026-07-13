-- Tags: no-parallel

DROP TABLE IF EXISTS users_03236_zero;
CREATE TABLE users_03236_zero (uid Int16, name String, num Int16) ENGINE=Memory;

INSERT INTO users_03236_zero VALUES (1231, 'John', 33);
INSERT INTO users_03236_zero VALUES (6666, 'John', 48);
INSERT INTO users_03236_zero VALUES (8888, 'Alice', 50);

select sum(num/toDecimal256(1000, 18)) from users_03236_zero;
