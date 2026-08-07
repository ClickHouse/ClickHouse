-- https://github.com/ClickHouse/ClickHouse/issues/10894

DROP TABLE IF EXISTS event;
DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS mv;

CREATE TABLE event (
    `event_time` DateTime,
    `event_name` String,
    `user_id` String
)
ENGINE = MergeTree()
ORDER BY (event_time, event_name);

CREATE TABLE user (
    `user_id` String,
    `user_type` String
)
ENGINE = MergeTree()
ORDER BY (user_id);

INSERT INTO event VALUES ('2020-05-01 00:00:01', 'install', '1'), ('2020-05-01 00:00:02', 'install', '2'), ('2020-05-01 00:00:03', 'install', '3');

INSERT INTO user VALUES ('1', 'type_1'), ('2', 'type_2'), ('3', 'type_3');

CREATE MATERIALIZED VIEW mv
(
    `event_time` DateTime,
    `event_name` String,
    `user_id` String,
    `user_type` String
)
ENGINE = MergeTree()
ORDER BY (event_time, event_name) POPULATE AS
SELECT
    e.event_time,
    e.event_name,
    e.user_id,
    u.user_type
FROM event e
INNER JOIN user u ON u.user_id = e.user_id;

DROP TABLE IF EXISTS event;
DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS mv;