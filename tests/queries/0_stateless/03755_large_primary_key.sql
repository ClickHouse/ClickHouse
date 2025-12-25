DROP TABLE IF EXISTS test_pk_long;
DROP TABLE IF EXISTS test_pk_long_back;
DROP TABLE IF EXISTS test_pk;

CREATE TABLE test_pk_long
(
    `dt` DateTime,
    `key` String
)
ENGINE = MergeTree
ORDER BY (dt, key, key || 'a', key || 'b', key || 'c', key || 'd', key || 'e', key || 'f', key || 'g', key || 'h', key || 'i', key || 'j', key || 'k', key || 'l', key || 'm', key || 'n', key || 'o', key || 'p', key || 'q', key || 'r', key || 's', key || 't')
SETTINGS index_granularity = 4;

CREATE TABLE test_pk_long_back
(
    `dt` DateTime,
    `key` String
)
ENGINE = MergeTree
ORDER BY (dt, key || 'a', key || 'b', key || 'c', key || 'd', key || 'e', key || 'f', key || 'g', key || 'h', key || 'i', key || 'j', key || 'k', key || 'l', key || 'm', key || 'n', key || 'o', key || 'p', key || 'q', key || 'r', key || 's', key || 't', key)
SETTINGS index_granularity = 4;

CREATE TABLE test_pk
(
    `dt` DateTime,
    `key` String
)
ENGINE = MergeTree
ORDER BY (dt, key)
SETTINGS index_granularity = 4;

INSERT INTO test_pk SELECT toDateTime('2001-01-01') + number, number % 16 FROM numbers_mt(100000);

INSERT INTO test_pk_long SELECT toDateTime('2001-01-01') + number, number % 16 FROM numbers_mt(100000);

INSERT INTO test_pk_long_back SELECT toDateTime('2001-01-01') + number, number % 16 FROM numbers_mt(100000);

-- { echoOn }

SELECT
    toDate(dt) AS dt,
    count()
FROM test_pk
WHERE (dt >= '2001-01-02') AND (dt <= '2001-01-11') AND (key = '1')
GROUP BY dt
ORDER BY dt ASC;

SELECT
    toDate(dt) AS dt,
    count()
FROM test_pk_long
WHERE (dt >= '2001-01-02') AND (dt <= '2001-01-11') AND (key = '1')
GROUP BY dt
ORDER BY dt ASC;

SELECT
    toDate(dt) AS dt,
    count()
FROM test_pk_long_back
WHERE (dt >= '2001-01-02') AND (dt <= '2001-01-11') AND (key = '1')
GROUP BY dt
ORDER BY dt ASC;
