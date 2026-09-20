SELECT
    sum(ws_net_paid) AS total_sum,
    i_category,
    i_class,
    grouping(i_category) + grouping(i_class) AS lochierarchy,
    rank() OVER (PARTITION BY grouping(i_category) + grouping(i_class), multiIf(grouping(i_class) = 0, i_category, NULL) ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
FROM web_sales, date_dim AS d1, item
WHERE ((d1.d_month_seq >= 1200) AND (d1.d_month_seq <= (1200 + 11)))
    AND (d1.d_date_sk = ws_sold_date_sk)
    AND (i_item_sk = ws_item_sk)
GROUP BY
    i_category,
    i_class
    WITH ROLLUP
ORDER BY
    lochierarchy DESC,
    multiIf(lochierarchy = 0, i_category, NULL),
    rank_within_parent
LIMIT 100;
