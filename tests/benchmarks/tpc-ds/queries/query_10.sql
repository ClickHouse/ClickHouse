SELECT
    cd_gender,
    cd_marital_status,
    cd_education_status,
    count(*) AS cnt1,
    cd_purchase_estimate,
    count(*) AS cnt2,
    cd_credit_rating,
    count(*) AS cnt3,
    cd_dep_count,
    count(*) AS cnt4,
    cd_dep_employed_count,
    count(*) AS cnt5,
    cd_dep_college_count,
    count(*) AS cnt6
FROM customer AS c, customer_address AS ca, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk) AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')) AND (cd_demo_sk = c.c_current_cdemo_sk) AND exists((
    SELECT *
    FROM store_sales, date_dim
    WHERE (c.c_customer_sk = ss_customer_sk) AND (ss_sold_date_sk = d_date_sk) AND (d_year = 2002) AND ((d_moy >= 1) AND (d_moy <= (1 + 3)))
)) AND (exists((
    SELECT *
    FROM web_sales, date_dim
    WHERE (c.c_customer_sk = ws_bill_customer_sk) AND (ws_sold_date_sk = d_date_sk) AND (d_year = 2002) AND ((d_moy >= 1) AND (d_moy <= (1 + 3)))
)) OR exists((
    SELECT *
    FROM catalog_sales, date_dim
    WHERE (c.c_customer_sk = cs_ship_customer_sk) AND (cs_sold_date_sk = d_date_sk) AND (d_year = 2002) AND ((d_moy >= 1) AND (d_moy <= (1 + 3)))
)))
GROUP BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
ORDER BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
LIMIT 100;