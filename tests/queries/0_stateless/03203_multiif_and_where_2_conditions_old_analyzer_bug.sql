DROP TABLE IF EXISTS bugcheck1;

CREATE TABLE bugcheck1
ENGINE = MergeTree
ORDER BY tuple()
AS SELECT
    'c1' as column_a,
    'c2' as column_b;

select 'this query used to be broken in old analyser:';
SELECT *,
  multiIf(column_b IN (SELECT 'c2' as someproduct), 'yes', 'no') AS condition_1,
  multiIf(column_b  = 'c2', 'true', 'false') AS condition_2
FROM (SELECT column_a, column_b FROM bugcheck1)
WHERE (condition_1 IN ('yes')) AND (condition_2 in ('true'))
SETTINGS enable_analyzer=0;

select 'this query worked:';

SELECT
  multiIf(column_b IN (SELECT 'c2' as someproduct), 'yes', 'no') AS condition_1,
  multiIf(column_b  = 'c2', 'true', 'false') AS condition_2
FROM (SELECT column_a, column_b FROM bugcheck1)
WHERE (condition_1 IN ('yes')) AND (condition_2 in ('true'))
SETTINGS enable_analyzer=0;

select 'experimental analyzer:';
SELECT *,
  multiIf(column_b IN (SELECT 'c2' as someproduct), 'yes', 'no') AS condition_1,
  multiIf(column_b  = 'c2', 'true', 'false') AS condition_2
FROM (SELECT column_a, column_b FROM bugcheck1)
WHERE (condition_1 IN ('yes')) AND (condition_2 in ('true'))
SETTINGS enable_analyzer=1;

DROP TABLE bugcheck1;
