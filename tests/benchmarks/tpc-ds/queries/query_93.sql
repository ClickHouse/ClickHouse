SELECT
    ss_customer_sk,
    sum(act_sales) AS sumsales
FROM
(
    SELECT
        ss_item_sk,
        ss_ticket_number,
        ss_customer_sk,
        multiIf(sr_return_quantity IS NOT NULL, (ss_quantity - sr_return_quantity) * ss_sales_price, ss_quantity * ss_sales_price) AS act_sales
    FROM store_sales
    LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk) AND (sr_ticket_number = ss_ticket_number), reason
    WHERE (sr_reason_sk = r_reason_sk)
        AND (r_reason_desc = 'reason 28')
) AS t
GROUP BY ss_customer_sk
ORDER BY
    sumsales,
    ss_customer_sk
LIMIT 100;
