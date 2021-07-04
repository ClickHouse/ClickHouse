SELECT empty(toUUID('00000000-0000-0000-0000-000000000000'));
SELECT uniqIf(uuid, empty(uuid))
FROM
(
    SELECT toUUID('00000000-0000-0000-0000-000000000002') AS uuid
    UNION ALL
    SELECT toUUID('00000000-0000-0000-0000-000000000001') AS uuid
);

CREATE DATABASE uuid_empty;
CREATE TABLE uuid_empty.users (user_id UUID) ENGINE = Memory;
CREATE TABLE uuid_empty.orders (order_id UUID, user_id UUID) ENGINE = Memory;
INSERT INTO uuid_empty.users VALUES ('00000000-0000-0000-0000-000000000001');
INSERT INTO uuid_empty.users VALUES ('00000000-0000-0000-0000-000000000002');
INSERT INTO uuid_empty.orders VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001');

SELECT
    uniq(user_id) AS users,
    uniqIf(order_id, notEmpty(order_id)) AS orders
FROM
(
    SELECT * FROM uuid_empty.users
) t1 ALL LEFT JOIN (
    SELECT * FROM uuid_empty.orders
) t2 USING (user_id);

DROP DATABASE uuid_empty;
