CREATE VIEW fake AS SELECT table, database, name FROM system.tables WHERE database = currentDatabase();

WITH
(`database` NOT LIKE 'system' and `name` = 'fake') AS `$condition`,
`$main` AS (SELECT DISTINCT table FROM fake WHERE `$condition`)
SELECT * FROM `$main`;

with properties_value[indexOf(properties_key, 'objectId')] as objectId,
data as (
  select
    ['objectId'] as properties_key,
    ['objectValue'] as properties_value
),
nested_query as (
  select
    objectId
  from
    data
)
select
  *
from
  nested_query;

WITH leftPad('abc', 4, '0') as paddedval
SELECT * FROM (SELECT paddedval);

with ('408','420') as some_tuple
select '408' in some_tuple as flag;

CREATE VIEW another_fake AS SELECT bytes, table FROM system.parts;

WITH
    sum(bytes) as s,
    data as (
      SELECT
        formatReadableSize(s),
        table
      FROM another_fake
      GROUP BY table
      ORDER BY s
    )
select * from data
FORMAT Null;

CREATE TABLE test
  (
    a UInt64,
    b UInt64,
    Block_Height UInt64,
    Block_Date Date
  ) ENGINE = Log;

WITH Block_Height BETWEEN 1 AND 2 AS block_filter
SELECT *
FROM test
WHERE block_filter
AND (
    Block_Date IN (
      SELECT Block_Date FROM test WHERE block_filter
    )
);

CREATE TABLE test_cte
(
    a UInt64,
    b UInt64,
)
ENGINE = MergeTree
ORDER BY tuple();

WITH
   (a > b) as cte,
   query AS
    (
        SELECT count()
        FROM test_cte
        WHERE cte
    )
SELECT *
FROM query;

WITH arrayMap(x -> (x + 1), [0]) AS a
SELECT 1
WHERE 1 IN (
    SELECT arrayJoin(a)
);
