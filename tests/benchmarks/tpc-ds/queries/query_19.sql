SELECT
    i_brand_id AS brand_id,
    i_brand AS brand,
    i_manufact_id,
    i_manufact,
    sum(ss_ext_sales_price) AS ext_price
FROM date_dim, store_sales, item, customer, customer_address, store
WHERE (d_date_sk = ss_sold_date_sk)
    AND (ss_item_sk = i_item_sk)
    AND (i_manager_id = 8)
    AND (d_moy = 11)
    AND (d_year = 1998)
    AND (ss_customer_sk = c_customer_sk)
    AND (c_current_addr_sk = ca_address_sk)
    AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
    AND (ss_store_sk = s_store_sk)
GROUP BY
    i_brand,
    i_brand_id,
    i_manufact_id,
    i_manufact
ORDER BY
    ext_price DESC,
    i_brand,
    i_brand_id,
    i_manufact_id,
    i_manufact
LIMIT 100;
