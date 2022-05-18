DROP TABLE IF EXISTS e;

-- https://github.com/ClickHouse/ClickHouse/issues/36891

CREATE TABLE e ( a UInt64, t DateTime ) ENGINE = MergeTree PARTITION BY toDate(t) ORDER BY tuple();

INSERT INTO e SELECT 1, toDateTime('2020-02-01 12:00:01') + INTERVAL number MONTH FROM numbers(10);

SELECT any('1')
FROM e JOIN ( SELECT 1 :: UInt32 AS key) AS da ON key = a
PREWHERE toString(a) = '1';

-- SELECT sumIf( 1, if( 1, toDateTime('2020-01-01 00:00:00', 'UTC'), toDateTime('1970-01-01 00:00:00', 'UTC')) > t )
-- FROM e JOIN ( SELECT 1 joinKey) AS da ON joinKey = a
-- WHERE  t >= toDateTime('2021-07-19T13:00:00', 'UTC') AND t <= toDateTime('2021-07-19T13:59:59', 'UTC');
