WITH
    ss AS
    (
        SELECT
            i_item_id,
            sum(ss_ext_sales_price) AS total_sales
        FROM store_sales, date_dim, customer_address, item
        WHERE (i_item_id IN (
            SELECT i_item_id
            FROM item
            WHERE (i_category IN ('Music'))
        ))
            AND (ss_item_sk = i_item_sk)
            AND (ss_sold_date_sk = d_date_sk)
            AND (d_year = 1998)
            AND (d_moy = 9)
            AND (ss_addr_sk = ca_address_sk)
            AND (ca_gmt_offset = -5)
        GROUP BY i_item_id
    ),
    cs AS
    (
        SELECT
            i_item_id,
            sum(cs_ext_sales_price) AS total_sales
        FROM catalog_sales, date_dim, customer_address, item
        WHERE (i_item_id IN (
            SELECT i_item_id
            FROM item
            WHERE (i_category IN ('Music'))
        ))
            AND (cs_item_sk = i_item_sk)
            AND (cs_sold_date_sk = d_date_sk)
            AND (d_year = 1998)
            AND (d_moy = 9)
            AND (cs_bill_addr_sk = ca_address_sk)
            AND (ca_gmt_offset = -5)
        GROUP BY i_item_id
    ),
    ws AS
    (
        SELECT
            i_item_id,
            sum(ws_ext_sales_price) AS total_sales
        FROM web_sales, date_dim, customer_address, item
        WHERE (i_item_id IN (
            SELECT i_item_id
            FROM item
            WHERE (i_category IN ('Music'))
        ))
            AND (ws_item_sk = i_item_sk)
            AND (ws_sold_date_sk = d_date_sk)
            AND (d_year = 1998)
            AND (d_moy = 9)
            AND (ws_bill_addr_sk = ca_address_sk)
            AND (ca_gmt_offset = -5)
        GROUP BY i_item_id
    )
SELECT
    i_item_id,
    sum(total_sales) AS total_sales
FROM
(
    SELECT * FROM ss
    UNION ALL
    SELECT * FROM cs
    UNION ALL
    SELECT * FROM ws
) AS tmp1
GROUP BY i_item_id
ORDER BY
    i_item_id,
    total_sales
LIMIT 100;
