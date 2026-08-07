CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

set enable_analyzer = 1;

WITH
  users AS (
    WITH t as (
      SELECT * FROM users
    )
    SELECT * FROM t
  )
SELECT *
FROM users
FORMAT Null;
