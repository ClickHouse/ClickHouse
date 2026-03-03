SELECT
    i_item_id,
    avg(ss_quantity) AS agg1,
    avg(ss_list_price) AS agg2,
    avg(ss_coupon_amt) AS agg3,
    avg(ss_sales_price) AS agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE (ss_sold_date_sk = d_date_sk)
    AND (ss_item_sk = i_item_sk)
    AND (ss_cdemo_sk = cd_demo_sk)
    AND (ss_promo_sk = p_promo_sk)
    AND (cd_gender = 'M')
    AND (cd_marital_status = 'S')
    AND (cd_education_status = 'College')
    AND ((p_channel_email = 'N') OR (p_channel_event = 'N'))
    AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;
