-- Tags: no-random-settings
-- `ORDER BY grade_name_id` sorts on a constant key (always 42), so the relative
-- order of the two UNION ALL branches is undefined and sensitive to pipeline
-- shape changes from `max_streams_for_union_step` etc.

WITH transactions_data AS
    (
        SELECT
            42 AS grade_name_id,
            42 AS today_flow_transaction_count,
            CAST('good', 'Nullable(String)') AS status
        FROM
        (
            SELECT 42 AS dispenser_id
        ) AS trans_his
        INNER JOIN
        (
            SELECT 42 AS dispenser_id
        ) AS gde ON trans_his.dispenser_id = gde.dispenser_id
    )
SELECT flag
FROM
(
    SELECT
        transactions_data.grade_name_id AS grade_name_id,
        multiIf(transactions_data.status = 'low', 'YELLOW', NULL) AS flag
    FROM transactions_data
    UNION ALL
    SELECT
        grade_name_id,
        multiIf(status = 'good', 'GREEN', NULL) AS flag
    FROM transactions_data
    WHERE status = 'good'
)
ORDER BY grade_name_id ASC;

