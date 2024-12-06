-- CASE 1
-- scores = [0.1, 0.4, 0.35, 0.8]
-- labels = [0, 0, 1, 1]

select floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);

with partials as (
  select arrayPrAUC(scores, labels, offsets) as partial_pr_auc from (
    select [0.8] as scores, [1] as labels, [0, 0, 2] as offsets
    UNION ALL
    select [0.4] as scores, [0] as labels, [1, 1, 2] as offsets
    UNION ALL
    select [0.35] as scores, [1] as labels, [1, 2, 2] as offsets
    UNION ALL
    select [0.1] as scores, [0] as labels, [2, 3, 2] as offsets
  )
)
select floor(sum(partial_pr_auc), 10) from partials;

with partials as (
  select arrayPrAUC(scores, labels, offsets) as partial_pr_auc from (
    select [0.8, 0.4] as scores, [1, 0] as labels, [0, 0, 2] as offsets
    UNION ALL
    select [0.35, 0.1] as scores, [1, 0] as labels, [1, 2, 2] as offsets
  )
)
select floor(sum(partial_pr_auc), 10) from partials;

-- CASE 2
-- scores = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
-- labels = [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]

select floor(arrayPrAUC([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10);

-- Example of a more robust query that can be used to calculate PR AUC for a large dataset
-- as long as the grouping is done correctly, i.e., no two groups have the same score and
-- the groups can be ordered by score.
WITH score_with_group AS (
  SELECT
    scores[idx] AS score,
    labels[idx] AS label,
    FLOOR(score / 3) as group
  FROM 
      (SELECT 
          range(1, 11) AS idx,
          [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] AS scores,
          [1, 0, 1, 0, 0, 0, 1, 0, 0, 1] AS labels)
  ARRAY JOIN idx
),
total_positives AS (
  SELECT
    countIf(label > 0) as total_positives
  FROM score_with_group
),
grouped_scores AS (
  SELECT
    group,
    groupArrayArray(array(score)) as scores,
    groupArrayArray(array(label)) as labels,
    countIf(label > 0) as group_tp,
    count(label) as group_total,
    COALESCE (SUM(group_tp) OVER (ORDER BY group DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prev_group_tp,
    COALESCE (SUM(group_total) OVER (ORDER BY group DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prev_group_total
  FROM score_with_group
  GROUP BY group
),
partial_pr_aucs AS (
  SELECT 
    arrayPrAUC(
      scores, 
      labels, 
      [
        COALESCE(prev_group_tp, 0), 
        COALESCE(prev_group_total, 0), 
        COALESCE((SELECT total_positives FROM total_positives), 0)
      ]
    ) as partial_pr_auc
  FROM grouped_scores
)
SELECT sum(partial_pr_auc) as pr_auc FROM partial_pr_aucs
