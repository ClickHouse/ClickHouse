SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    sum(ws_ext_sales_price) AS itemrevenue,
    (sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales, item, date_dim
WHERE (ws_item_sk = i_item_sk) AND (i_category IN ('Sports', 'Books', 'Home')) AND (ws_sold_date_sk = d_date_sk) AND ((d_date >= CAST('1999-02-22', 'date')) AND (d_date <= (CAST('1999-02-22', 'date') + INTERVAL 30 day)))
GROUP BY
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100;

