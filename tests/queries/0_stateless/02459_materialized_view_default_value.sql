DROP TABLE IF EXISTS session;
DROP TABLE IF EXISTS queue;
DROP TABLE IF EXISTS forward;

CREATE TABLE session
(
    `day` Date,
    `uid` String,
    `dummy` String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (day, uid);

CREATE TABLE queue
(
    `day` Date,
    `uid` String
)
ENGINE = MergeTree
ORDER BY (day, uid);

CREATE MATERIALIZED VIEW IF NOT EXISTS forward TO session AS
SELECT
    day,
    uid
FROM queue;

insert into queue values ('2019-05-01', 'test');

SELECT * FROM queue;
SELECT * FROM session;
SELECT * FROM forward;

DROP TABLE session;
DROP TABLE queue;
DROP TABLE forward;
