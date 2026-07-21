-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/91946
-- `NOT_FOUND_COLUMN_IN_BLOCK` when performing grouped `SELECT` on `ReplacingMergeTree FINAL` with a row policy attached.

DROP ROW POLICY IF EXISTS users_91946_policy ON users_91946;
DROP TABLE IF EXISTS users_91946;

CREATE TABLE users_91946
(
    uid Int16,
    name String,
    age Int16,
    deleted UInt8,
    version UInt64
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY uid
ORDER BY uid;

INSERT INTO users_91946 VALUES (1231, 'John',   33, 0, 1);
INSERT INTO users_91946 VALUES (6666, 'Ksenia', 48, 0, 1);
INSERT INTO users_91946 VALUES (8888, 'Alice',  20, 0, 1);
INSERT INTO users_91946 VALUES (1010, 'Jack',   50, 0, 1);
INSERT INTO users_91946 VALUES (8288, 'Peter',  37, 0, 1);
INSERT INTO users_91946 VALUES (8818, 'Sarah',  39, 0, 1);

CREATE ROW POLICY users_91946_policy ON users_91946 USING deleted = 0 TO ALL;

SELECT age
FROM users_91946 FINAL
WHERE age > 30 AND age < 40
GROUP BY age
ORDER BY age;

DROP ROW POLICY users_91946_policy ON users_91946;
DROP TABLE users_91946;
