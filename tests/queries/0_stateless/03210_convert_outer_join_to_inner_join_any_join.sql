DROP TABLE IF EXISTS user_country;
DROP TABLE IF EXISTS user_transactions;

CREATE TABLE user_country (
    user_id UInt64,
    country String
)
ENGINE = ReplacingMergeTree
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
