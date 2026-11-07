
-- CASE 1
-- scores = [0.1, 0.4, 0.35, 0.8]
-- labels = [0, 0, 1, 1]

select 
  floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10), 
  floor(arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);

with partial_aucs as (
  select 
    arrayAUCPR(scores, labels, pr_offsets) as partial_pr_auc,
    arrayROCAUC(scores, labels, true, roc_offsets) as partial_roc_auc
  from (
    select [0.8] as scores, [1] as labels, [0, 0, 2] as pr_offsets, [0, 0, 2, 2] as roc_offsets
    UNION ALL
    select [0.4] as scores, [0] as labels, [1, 0, 2] as pr_offsets, [1, 0, 2, 2] as roc_offsets
    UNION ALL
    select [0.35] as scores, [1] as labels, [1, 1, 2] as pr_offsets, [1, 1, 2, 2] as roc_offsets
    UNION ALL
    select [0.1] as scores, [0] as labels, [2, 1, 2] as pr_offsets, [2, 1, 2, 2] as roc_offsets
  )
)
select 
  floor(sum(partial_pr_auc), 10),
  floor(sum(partial_roc_auc), 10)
from partial_aucs;

with partial_aucs as (
  select
    arrayAUCPR(scores, labels, pr_offsets) as partial_pr_auc,
    arrayROCAUC(scores, labels, true, roc_offsets) as partial_roc_auc
  from (
    select [0.8, 0.4] as scores, [1, 0] as labels, [0, 0, 2] as pr_offsets, [0, 0, 2, 2] as roc_offsets
    UNION ALL
    select [0.35, 0.1] as scores, [1, 0] as labels, [1, 1, 2] as pr_offsets, [1, 1, 2, 2] as roc_offsets
  )
)
select 
  floor(sum(partial_pr_auc), 10),
  floor(sum(partial_roc_auc), 10)
from partial_aucs;

-- CASE 2
-- scores = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
-- labels = [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]

select 
  floor(arrayAUCPR([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10),
  floor(arrayROCAUC([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 0, 1, 0, 0, 0, 1, 0, 0, 1]), 10);

-- Example of a more robust query that can be used to calculate AUC for a large dataset
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
grouped_scores AS (
  SELECT
    group,
    groupArrayArray(array(score)) as scores,
    groupArrayArray(array(label)) as labels,
    countIf(label > 0) as group_tp,
    countIf(label = 0) as group_fp,
    COALESCE (SUM(group_tp) OVER (ORDER BY group DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prev_group_tp,
    COALESCE (SUM(group_fp) OVER (ORDER BY group DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS prev_group_fp
  FROM score_with_group
  GROUP BY group
),
partial_aucs AS (
  SELECT 
    arrayAUCPR(
      scores, 
      labels, 
      [
        COALESCE(prev_group_tp, 0), 
        COALESCE(prev_group_fp, 0), 
        COALESCE(SUM(group_tp) OVER(), 0)
      ]
    ) as partial_pr_auc,
    arrayROCAUC(
      scores, 
      labels,
      true,
      [
        COALESCE(prev_group_tp, 0), 
        COALESCE(prev_group_fp, 0),  
        COALESCE(SUM(group_tp) OVER (), 0),
        COALESCE(SUM(group_fp) OVER (), 0)
      ]
    ) as partial_roc_auc
  FROM grouped_scores
)
SELECT 
  floor(sum(partial_pr_auc), 10) as pr_auc,
  floor(sum(partial_roc_auc), 10) as roc_auc
FROM partial_aucs
