WITH
(`database` LIKE 'system' and `name` = 'one') AS `$condition`,
`$main` AS (SELECT DISTINCT table FROM system.tables WHERE `$condition`)
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

WITH sum(bytes) as s,
data as (
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
)
select * from data
FORMAT Null;

with ('408','420') as some_tuple
select '408' in some_tuple as flag;

-- create or replace view v_test1 as
-- with ('408','420') as some_tuple
-- select '408' in some_tuple as flag;

-- WITH dummy BETWEEN 1 AND 2 AS block_filter
-- SELECT *
-- FROM system.one
-- WHERE block_filter 
-- AND (
--     dummy IN (
--       SELECT dummy FROM system.one WHERE block_filter
--     )
-- );
