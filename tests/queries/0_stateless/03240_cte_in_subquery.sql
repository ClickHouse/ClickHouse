SET enable_analyzer = 1;

DROP TABLE IF EXISTS subquery_cte_in;

CREATE TABLE subquery_cte_in
(
    `date` DateTime64(3),
    `label` UInt32,
    `id` UInt32
)
ENGINE = MergeTree
ORDER BY (label, id, date);

INSERT INTO subquery_cte_in VALUES (toDateTime('2023-10-24 16:13:38'), 2, 6), (toDateTime('2023-10-24 16:00:00'), 2, 10), (toDateTime('2023-10-24 00:00:00'), 2, 6);

SELECT max(date_out)
FROM
(
    WITH
        (
            SELECT max(date)
            FROM subquery_cte_in 
            WHERE (id = 6) AND (label = 2)
        ) AS cte_1,
        (
            SELECT max(date)
            FROM subquery_cte_in 
            WHERE (id = 10) AND (label = 2)
        ) AS cte_2
    SELECT date AS date_out
    FROM subquery_cte_in
    WHERE date IN (cte_1, cte_2)
);

DROP TABLE subquery_cte_in;
