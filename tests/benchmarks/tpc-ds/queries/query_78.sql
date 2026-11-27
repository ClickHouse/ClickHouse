WITH
    ws AS
    (
        SELECT
            d_year AS ws_sold_year,
            ws_item_sk,
            ws_bill_customer_sk AS ws_customer_sk,
            sum(ws_quantity) AS ws_qty,
            sum(ws_wholesale_cost) AS ws_wc,
            sum(ws_sales_price) AS ws_sp
        FROM web_sales
        LEFT JOIN web_returns ON (wr_order_number = ws_order_number) AND (ws_item_sk = wr_item_sk)
        INNER JOIN date_dim ON ws_sold_date_sk = d_date_sk
        WHERE wr_order_number IS NULL
        GROUP BY
            d_year,
            ws_item_sk,
            ws_bill_customer_sk
    ),
    cs AS
    (
        SELECT
            d_year AS cs_sold_year,
            cs_item_sk,
            cs_bill_customer_sk AS cs_customer_sk,
            sum(cs_quantity) AS cs_qty,
            sum(cs_wholesale_cost) AS cs_wc,
            sum(cs_sales_price) AS cs_sp
        FROM catalog_sales
        LEFT JOIN catalog_returns ON (cr_order_number = cs_order_number) AND (cs_item_sk = cr_item_sk)
        INNER JOIN date_dim ON cs_sold_date_sk = d_date_sk
        WHERE cr_order_number IS NULL
        GROUP BY
            d_year,
            cs_item_sk,
            cs_bill_customer_sk
    ),
    ss AS
    (
        SELECT
            d_year AS ss_sold_year,
            ss_item_sk,
            ss_customer_sk,
            sum(ss_quantity) AS ss_qty,
            sum(ss_wholesale_cost) AS ss_wc,
            sum(ss_sales_price) AS ss_sp
        FROM store_sales
        LEFT JOIN store_returns ON (sr_ticket_number = ss_ticket_number) AND (ss_item_sk = sr_item_sk)
        INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE sr_ticket_number IS NULL
        GROUP BY
            d_year,
            ss_item_sk,
            ss_customer_sk
    )
SELECT
    ss_sold_year,
    ss_item_sk,
    ss_customer_sk,
    round(ss_qty / (coalesce(ws_qty, 0) + coalesce(cs_qty, 0)), 2) AS ratio,
    ss_qty AS store_qty,
    ss_wc AS store_wholesale_cost,
    ss_sp AS store_sales_price,
    coalesce(ws_qty, 0) + coalesce(cs_qty, 0) AS other_chan_qty,
    coalesce(ws_wc, 0) + coalesce(cs_wc, 0) AS other_chan_wholesale_cost,
    coalesce(ws_sp, 0) + coalesce(cs_sp, 0) AS other_chan_sales_price
FROM ss
LEFT JOIN ws ON (ws_sold_year = ss_sold_year) AND (ws_item_sk = ss_item_sk) AND (ws_customer_sk = ss_customer_sk)
LEFT JOIN cs ON (cs_sold_year = ss_sold_year) AND (cs_item_sk = ss_item_sk) AND (cs_customer_sk = ss_customer_sk)
WHERE ((coalesce(ws_qty, 0) > 0) OR (coalesce(cs_qty, 0) > 0)) AND (ss_sold_year = 2000)
ORDER BY
    ss_sold_year,
    ss_item_sk,
    ss_customer_sk,
    ss_qty DESC,
    ss_wc DESC,
    ss_sp DESC,
    other_chan_qty,
    other_chan_wholesale_cost,
    other_chan_sales_price,
    ratio
LIMIT 100;