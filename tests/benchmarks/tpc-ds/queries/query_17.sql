-- Returns nan instead of NULL as stddev_samp is called on a single value: https://github.com/ClickHouse/ClickHouse/issues/94683

SELECT
    i_item_id,
    i_item_desc,
    s_state,
    count(ss_quantity) AS store_sales_quantitycount,
    avg(ss_quantity) AS store_sales_quantityave,
    stddev_samp(ss_quantity) AS store_sales_quantitystdev,
    stddev_samp(ss_quantity) / avg(ss_quantity) AS store_sales_quantitycov,
    count(sr_return_quantity) AS store_returns_quantitycount,
    avg(sr_return_quantity) AS store_returns_quantityave,
    stddev_samp(sr_return_quantity) AS store_returns_quantitystdev,
    stddev_samp(sr_return_quantity) / avg(sr_return_quantity) AS store_returns_quantitycov,
    count(cs_quantity) AS catalog_sales_quantitycount,
    avg(cs_quantity) AS catalog_sales_quantityave,
    stddev_samp(cs_quantity) AS catalog_sales_quantitystdev,
    stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitycov
FROM store_sales, store_returns, catalog_sales, date_dim AS d1, date_dim AS d2, date_dim AS d3, store, item
WHERE (d1.d_quarter_name = '2001Q1')
    AND (d1.d_date_sk = ss_sold_date_sk)
    AND (i_item_sk = ss_item_sk)
    AND (s_store_sk = ss_store_sk)
    AND (ss_customer_sk = sr_customer_sk)
    AND (ss_item_sk = sr_item_sk)
    AND (ss_ticket_number = sr_ticket_number)
    AND (sr_returned_date_sk = d2.d_date_sk)
    AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
    AND (sr_customer_sk = cs_bill_customer_sk)
    AND (sr_item_sk = cs_item_sk)
    AND (cs_sold_date_sk = d3.d_date_sk)
    AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
GROUP BY
    i_item_id,
    i_item_desc,
    s_state
ORDER BY
    i_item_id,
    i_item_desc,
    s_state
LIMIT 100;
