SET enable_analyzer = 1;

SELECT grouping(num1), num1,
       any(num1), min(num1), max(num1), sum(num1), avg(num1), count(num1),
       any(num2), min(num2), max(num2), sum(num2), avg(num2), count(num2)
FROM (SELECT 10 AS num1, 20 AS num2)
GROUP BY GROUPING SETS ((num1), ())
ORDER BY grouping(num1) DESC;

DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16, ts DateTime) ENGINE=MergeTree order by tuple();

INSERT INTO users VALUES (1231, 'John', 1, toDateTime('2025-10-11 12:13:14'));
INSERT INTO users VALUES (1231, 'John', 2, toDateTime('2025-10-11 12:13:14') + 1);
INSERT INTO users VALUES (1231, 'John', 3 , toDateTime('2025-10-11 12:13:14') + 2);

INSERT INTO users VALUES (6666, 'Ksenia', 1, toDateTime('2025-10-11 12:13:14') + 3);
INSERT INTO users VALUES (6666, 'Ksenia', 2, toDateTime('2025-10-11 12:13:14') + 4);

INSERT INTO users VALUES (8888, 'Alice', 1, toDateTime('2025-10-11 12:13:14') + 5);

select arrayStringConcat(groupArray('-')) from numbers(67);

-- Query A
select 
    uid, name
    ,sum(age)
    ,count()
    ,arrayUniq(groupArray(ts))
    ,max(age)
    ,max(ts)
from users
group by grouping sets 
(
    (*),
    ()
)
ORDER BY ALL;

select arrayStringConcat(groupArray('-')) from numbers(67);

-- Query B
select 
    uid, name
    ,sum(age)
    ,count()
    ,arrayUniq(groupArray(ts))
    ,max(age)
    ,max(ts)
from users
group by grouping sets 
(
    (uid, name),
    ()
)
ORDER BY ALL;

DROP TABLE users;
