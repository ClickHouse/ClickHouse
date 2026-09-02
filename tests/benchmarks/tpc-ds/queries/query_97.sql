WITH
    ssci AS
    (
        SELECT
            ss_customer_sk AS customer_sk,
            ss_item_sk AS item_sk
        FROM store_sales, date_dim
        WHERE (ss_sold_date_sk = d_date_sk)
            AND (d_month_seq BETWEEN 1200 AND 1200 + 11)
        GROUP BY
            ss_customer_sk,
            ss_item_sk
    ),
    csci AS
    (
        SELECT
            cs_bill_customer_sk AS customer_sk,
            cs_item_sk AS item_sk
        FROM catalog_sales, date_dim
        WHERE (cs_sold_date_sk = d_date_sk)
            AND (d_month_seq BETWEEN 1200 AND 1200 + 11)
        GROUP BY
            cs_bill_customer_sk,
            cs_item_sk
    )
SELECT
    sum(CASE WHEN (ssci.customer_sk IS NOT NULL) AND (csci.customer_sk IS NULL) THEN 1 ELSE 0 END) AS store_only,
    sum(CASE WHEN (ssci.customer_sk IS NULL) AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END) AS catalog_only,
    sum(CASE WHEN (ssci.customer_sk IS NOT NULL) AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END) AS store_and_catalog
FROM ssci
FULL OUTER JOIN csci ON (ssci.customer_sk = csci.customer_sk) AND (ssci.item_sk = csci.item_sk)
LIMIT 100;
