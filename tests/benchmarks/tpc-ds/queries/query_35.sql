SELECT
    ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    count(*) AS cnt1,
    avg(cd_dep_count),
    min(cd_dep_count),
    min(cd_dep_count),
    cd_dep_employed_count,
    count(*) AS cnt2,
    avg(cd_dep_employed_count),
    min(cd_dep_employed_count),
    min(cd_dep_employed_count),
    cd_dep_college_count,
    count(*) AS cnt3,
    avg(cd_dep_college_count),
    min(cd_dep_college_count),
    min(cd_dep_college_count)
FROM customer AS c, customer_address AS ca, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
    AND (cd_demo_sk = c.c_current_cdemo_sk)
    AND c.c_customer_sk IN (
        SELECT ss_customer_sk
        FROM store_sales, date_dim
        WHERE (ss_sold_date_sk = d_date_sk)
            AND (d_year = 2002)
            AND (d_qoy < 4)
    )
    AND (
        c.c_customer_sk IN (
            SELECT ws_bill_customer_sk
            FROM web_sales, date_dim
            WHERE (ws_sold_date_sk = d_date_sk)
                AND (d_year = 2002)
                AND (d_qoy < 4)
        )
        OR c.c_customer_sk IN (
            SELECT cs_ship_customer_sk
            FROM catalog_sales, date_dim
            WHERE (cs_sold_date_sk = d_date_sk)
                AND (d_year = 2002)
                AND (d_qoy < 4)
        )
    )
GROUP BY
    ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
ORDER BY
    ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
LIMIT 100;
