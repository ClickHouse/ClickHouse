DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int32, c1 Int32, c2 String) ENGINE = Log() ;
INSERT INTO t0(c0, c1, c2) VALUES (826636805,0, ''), (0, 150808457, '');

SELECT 'DATA';
SELECT * FROM t0 FORMAT PrettyMonoBlock;

SELECT 'NUMBER OF ROWS IN FIRST SHOULD BE EQUAL TO SECOND';


SELECT 'FISRT';
SELECT left.c2 FROM t0 AS left
LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1))
WHERE (abs ((- ((sign (right_0.c1))))));

SELECT 'SECOND';
SELECT SUM(check <> 0) 
FROM 
(
  SELECT (abs ((- ((sign (right_0.c1)))))) AS `check`
  FROM t0 AS left 
  LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1))
);


SELECT 'TO DEBUG I TOOK JUST A SUBQUERY AND IT HAS 1 ROW';

SELECT 'THIRD';

SELECT (abs ((- ((sign (right_0.c1)))))) AS `check` 
FROM t0 AS left 
LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1));


SELECT 'AND I ADDED SINGLE CONDITION THAT CONDITION <>0 THAT IS 1 IN THIRD QUERY AND IT HAS NO RESULT!!!';


SELECT 'FOURTH';
SELECT (abs ((- ((sign (right_0.c1)))))) AS `check` 
FROM t0 AS left 
LEFT ANTI JOIN t0 AS right_0 ON ((left.c0)=(right_0.c1))
WHERE check <> 0;

DROP TABLE t0;


DROP TABLE IF EXISTS user_country;
DROP TABLE IF EXISTS user_transactions;

CREATE TABLE user_country (
    user_id UInt64,
    country String,
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY user_id;

CREATE TABLE user_transactions (
    user_id UInt64,
    transaction_id String
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO user_country (user_id, country) VALUES (1, 'US');
INSERT INTO user_transactions (user_id, transaction_id) VALUES (1, 'tx1'), (1, 'tx2'), (1, 'tx3'), (2, 'tx1');

-- Expected 3 rows, got only 1. Removing 'ANY' and adding 'FINAL' fixes
-- the issue (but it is not always possible). Moving filter by 'country' to
-- an outer query doesn't help. Query without filter by 'country' works
-- as expected (returns 3 rows).
SELECT * FROM user_transactions
ANY LEFT JOIN user_country USING (user_id)
WHERE
    user_id = 1
    AND country = 'US'
ORDER BY ALL;

DROP TABLE user_country;
DROP TABLE user_transactions;
