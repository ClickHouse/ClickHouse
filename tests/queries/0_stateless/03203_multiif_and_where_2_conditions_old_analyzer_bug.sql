SET optimize_multiif_to_if = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_and_compare_chain = 1;
SET use_query_condition_cache = 0;
SET optimize_extract_common_expressions = 0;
SET optimize_respect_aliases = 1;
SET optimize_if_transform_strings_to_enum = 0;
SET optimize_if_chain_to_multiif = 0;
SET optimize_skip_unused_shards = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET optimize_use_projection_filtering = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_aggregators_of_group_by_keys = 1;

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
