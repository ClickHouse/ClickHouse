SET enable_analyzer=1;
SET allow_experimental_correlated_subqueries = 0;

with
    arrayMap(x -> x + 1, [0]) as a
select
    1
where
    1 in (select arrayJoin(a));

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 10);
INSERT INTO users VALUES (6666, 'Ksenia', 20);
INSERT INTO users VALUES (8888, 'Alice', 30);

CREATE OR REPLACE FUNCTION oldest_before AS (age_max) -> (
  SELECT uid FROM users WHERE users.age < age_max ORDER BY users.age DESC LIMIT 1
);

SELECT *, oldest_before(users.age)
FROM users; -- { serverError UNSUPPORTED_METHOD }
