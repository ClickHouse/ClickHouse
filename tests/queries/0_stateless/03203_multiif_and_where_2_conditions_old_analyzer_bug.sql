CREATE OR REPLACE TABLE bugcheck1
ENGINE = MergeTree
ORDER BY tuple()
AS SELECT
    'c1' as column_a,
    'c2' as column_b;

select 'this query used to be broken in old analyser:';
SELECT
    *,
      multiIf(column_b IN (
        SELECT 'c2' as someproduct
    ), 'yes', 'no') AS condition_1,
    multiIf(column_b  = 'c2', 'true', 'false') AS condition_2,
  --condition in WHERE is true
  (condition_1 IN ('yes')) AND (condition_2 in ('true')) as cond
FROM
(
    SELECT column_a, column_b
    FROM bugcheck1
)
WHERE (condition_1 IN ('yes')) AND (condition_2 in ('true'))
settings allow_experimental_analyzer=0;

select 'this query worked:';
SELECT
    *,
      multiIf(column_b IN (
        SELECT 'c2' as someproduct
    ), 'yes', 'no') AS condition_1,
    multiIf(column_b  = 'c2', 'true', 'false') AS condition_2,
  --condition in WHERE is true
  (condition_1 IN ('yes')) AND (condition_2 in ('true')) as cond
FROM
(
    SELECT column_a, column_b
    FROM bugcheck1
)
--the next line is the only difference:
-- WHERE (condition_1 IN ('yes')) AND (condition_2 in ('true'))
WHERE (condition_2 in ('true'))
settings allow_experimental_analyzer=0;
