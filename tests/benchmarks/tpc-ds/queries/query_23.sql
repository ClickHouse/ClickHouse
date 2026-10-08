WITH
    frequent_ss_items AS
    (
        SELECT
            substr(i_item_desc, 1, 30) AS itemdesc,
            i_item_sk AS item_sk,
            d_date AS solddate,
            count(*) AS cnt
        FROM store_sales, date_dim, item
        WHERE (ss_sold_date_sk = d_date_sk)
            AND (ss_item_sk = i_item_sk)
            AND (d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3))
        GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
        HAVING count(*) > 4
    ),
    max_store_sales AS
    (
        SELECT max(csales) AS tpcds_cmax
        FROM
        (
            SELECT
                c_customer_sk,
                sum(ss_quantity * ss_sales_price) AS csales
            FROM store_sales, customer, date_dim
            WHERE (ss_customer_sk = c_customer_sk)
                AND (ss_sold_date_sk = d_date_sk)
                AND (d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3))
            GROUP BY c_customer_sk
        )
    ),
    best_ss_customer AS
    (
        SELECT
            c_customer_sk,
            sum(ss_quantity * ss_sales_price) AS ssales
        FROM store_sales, customer
        WHERE (ss_customer_sk = c_customer_sk)
        GROUP BY c_customer_sk
        HAVING sum(ss_quantity * ss_sales_price) > (50 / 100.0) * (
            SELECT *
            FROM max_store_sales
        )
    )
SELECT sum(sales)
FROM
(
    SELECT cs_quantity * cs_list_price AS sales
    FROM catalog_sales, date_dim
    WHERE (d_year = 2000)
        AND (d_moy = 2)
        AND (cs_sold_date_sk = d_date_sk)
        AND (cs_item_sk IN (SELECT item_sk FROM frequent_ss_items))
        AND (cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer))
    UNION ALL
    SELECT ws_quantity * ws_list_price AS sales
    FROM web_sales, date_dim
    WHERE (d_year = 2000)
        AND (d_moy = 2)
        AND (ws_sold_date_sk = d_date_sk)
        AND (ws_item_sk IN (SELECT item_sk FROM frequent_ss_items))
        AND (ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer))
)
LIMIT 100;

WITH
    frequent_ss_items AS
    (
        SELECT
            substr(i_item_desc, 1, 30) AS itemdesc,
            i_item_sk AS item_sk,
            d_date AS solddate,
            count(*) AS cnt
        FROM store_sales, date_dim, item
        WHERE (ss_sold_date_sk = d_date_sk)
            AND (ss_item_sk = i_item_sk)
            AND (d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3))
        GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
        HAVING count(*) > 4
    ),
    max_store_sales AS
    (
        SELECT max(csales) AS tpcds_cmax
        FROM
        (
            SELECT
                c_customer_sk,
                sum(ss_quantity * ss_sales_price) AS csales
            FROM store_sales, customer, date_dim
            WHERE (ss_customer_sk = c_customer_sk)
                AND (ss_sold_date_sk = d_date_sk)
                AND (d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3))
            GROUP BY c_customer_sk
        )
    ),
    best_ss_customer AS
    (
        SELECT
            c_customer_sk,
            sum(ss_quantity * ss_sales_price) AS ssales
        FROM store_sales, customer
        WHERE (ss_customer_sk = c_customer_sk)
        GROUP BY c_customer_sk
        HAVING sum(ss_quantity * ss_sales_price) > (50 / 100.0) * (
            SELECT *
            FROM max_store_sales
        )
    )
SELECT
    c_last_name,
    c_first_name,
    sales
FROM
(
    SELECT
        c_last_name,
        c_first_name,
        sum(cs_quantity * cs_list_price) AS sales
    FROM catalog_sales, customer, date_dim
    WHERE (d_year = 2000)
        AND (d_moy = 2)
        AND (cs_sold_date_sk = d_date_sk)
        AND (cs_item_sk IN (SELECT item_sk FROM frequent_ss_items))
        AND (cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer))
        AND (cs_bill_customer_sk = c_customer_sk)
    GROUP BY c_last_name, c_first_name
    UNION ALL
    SELECT
        c_last_name,
        c_first_name,
        sum(ws_quantity * ws_list_price) AS sales
    FROM web_sales, customer, date_dim
    WHERE (d_year = 2000)
        AND (d_moy = 2)
        AND (ws_sold_date_sk = d_date_sk)
        AND (ws_item_sk IN (SELECT item_sk FROM frequent_ss_items))
        AND (ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer))
        AND (ws_bill_customer_sk = c_customer_sk)
    GROUP BY c_last_name, c_first_name
)
ORDER BY c_last_name, c_first_name, sales
LIMIT 100;
