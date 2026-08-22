drop table if exists users_items;
CREATE TABLE users_items (user_id UInt64) ENGINE = Log;
INSERT INTO users_items SELECT bitAnd(number, 15) from numbers(64);

SELECT sum(in_sample)
FROM
(
    WITH RandomUsers AS
        (
            SELECT
                user_id,
                rand() % 2 AS in_sample
            FROM users_items
            GROUP BY user_id
        )
    SELECT
        user_id,
        in_sample
    FROM RandomUsers
    WHERE in_sample = 0
);
