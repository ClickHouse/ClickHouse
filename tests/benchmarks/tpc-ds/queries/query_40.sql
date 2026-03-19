SELECT
    w_state,
    i_item_id,
    sum(CASE WHEN (CAST(d_date AS date) < CAST('2000-03-11' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash, 0) ELSE 0 END) AS sales_before,
    sum(CASE WHEN (CAST(d_date AS date) >= CAST('2000-03-11' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash, 0) ELSE 0 END) AS sales_after
FROM catalog_sales
LEFT OUTER JOIN catalog_returns ON (cs_order_number = cr_order_number) AND (cs_item_sk = cr_item_sk),
warehouse, item, date_dim
WHERE (i_current_price BETWEEN 0.99 AND 1.49)
    AND (i_item_sk = cs_item_sk)
    AND (cs_warehouse_sk = w_warehouse_sk)
    AND (cs_sold_date_sk = d_date_sk)
    AND (d_date BETWEEN (CAST('2000-03-11' AS date) - INTERVAL 30 DAY) AND (CAST('2000-03-11' AS date) + INTERVAL 30 DAY))
GROUP BY w_state, i_item_id
ORDER BY w_state, i_item_id
LIMIT 100;
