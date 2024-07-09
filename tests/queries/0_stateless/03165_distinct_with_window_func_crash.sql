DROP TABLE IF EXISTS atable;

CREATE TABLE atable
(
    cdu_date Int16,
    loanx_id String,
    rating_sp String
)
ENGINE = MergeTree
ORDER BY tuple();

-- max_threads is randomized, and can significantly increase number of parallel transformers after window func, so set to small value explicitly
SET max_threads=3;

SELECT DISTINCT
    loanx_id,
    rating_sp,
    cdu_date,
    row_number() OVER (PARTITION BY cdu_date) AS row_number,
    last_value(cdu_date) OVER (PARTITION BY loanx_id ORDER BY cdu_date ASC) AS last_cdu_date
FROM atable
GROUP BY
    cdu_date,
    loanx_id,
    rating_sp
SETTINGS query_plan_remove_redundant_distinct = 1;

DROP TABLE atable;
