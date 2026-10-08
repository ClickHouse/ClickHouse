SELECT
    i_item_desc,
    w_warehouse_name,
    d1.d_week_seq,
    sum(multiIf(p_promo_sk IS NULL, 1, 0)) AS no_promo,
    sum(multiIf(p_promo_sk IS NOT NULL, 1, 0)) AS promo,
    count(*) AS total_cnt
FROM catalog_sales
INNER JOIN inventory ON cs_item_sk = inv_item_sk
INNER JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
INNER JOIN item ON i_item_sk = cs_item_sk
INNER JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
INNER JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
INNER JOIN date_dim AS d1 ON cs_sold_date_sk = d1.d_date_sk
INNER JOIN date_dim AS d2 ON inv_date_sk = d2.d_date_sk
INNER JOIN date_dim AS d3 ON cs_ship_date_sk = d3.d_date_sk
LEFT JOIN promotion ON cs_promo_sk = p_promo_sk
LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk) AND (cr_order_number = cs_order_number)
WHERE (d1.d_week_seq = d2.d_week_seq)
    AND (inv_quantity_on_hand < cs_quantity)
    AND (d3.d_date > (d1.d_date + 5))
    AND (hd_buy_potential = '>10000')
    AND (d1.d_year = 1999)
    AND (cd_marital_status = 'D')
GROUP BY
    i_item_desc,
    w_warehouse_name,
    d_week_seq
ORDER BY
    total_cnt DESC,
    i_item_desc,
    w_warehouse_name,
    d_week_seq
LIMIT 100;
