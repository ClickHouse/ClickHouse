-- The result contains ᴺᵁᴸᴸ. This is correct, but we were using format_tsv_null_representation='' setting to represent nulls as empty strings
-- https://github.com/ClickHouse/ClickHouse/issues/95168
-- Note: running this query for the first time may produce '', but consecutive runs will produce 'ᴺᵁᴸᴸ'
WITH
    web_v1 AS
    (
        SELECT
            ws_item_sk AS item_sk,
            d_date,
            sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
        FROM web_sales, date_dim
        WHERE (ws_sold_date_sk = d_date_sk)
            AND (d_month_seq BETWEEN 1200 AND 1200 + 11)
            AND (ws_item_sk IS NOT NULL)
        GROUP BY ws_item_sk, d_date
    ),
    store_v1 AS
    (
        SELECT
            ss_item_sk AS item_sk,
            d_date,
            sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
        FROM store_sales, date_dim
        WHERE (ss_sold_date_sk = d_date_sk)
            AND (d_month_seq BETWEEN 1200 AND 1200 + 11)
            AND (ss_item_sk IS NOT NULL)
        GROUP BY ss_item_sk, d_date
    )
SELECT *
FROM
(
    SELECT
        item_sk,
        d_date,
        web_sales,
        store_sales,
        max(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cumulative,
        max(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cumulative
    FROM
    (
        SELECT
            CASE WHEN web.item_sk IS NOT NULL THEN web.item_sk ELSE store.item_sk END AS item_sk,
            CASE WHEN web.d_date IS NOT NULL THEN web.d_date ELSE store.d_date END AS d_date,
            web.cume_sales AS web_sales,
            store.cume_sales AS store_sales
        FROM web_v1 AS web
        FULL OUTER JOIN store_v1 AS store ON (web.item_sk = store.item_sk) AND (web.d_date = store.d_date)
    ) AS x
) AS y
WHERE (web_cumulative > store_cumulative)
ORDER BY item_sk, d_date
LIMIT 100;
