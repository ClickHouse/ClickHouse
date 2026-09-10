SELECT
    c_last_name,
    c_first_name,
    substr(s_city, 1, 30),
    ss_ticket_number,
    amt,
    profit
FROM
(
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        store.s_city,
        sum(ss_coupon_amt) AS amt,
        sum(ss_net_profit) AS profit
    FROM store_sales, date_dim, store, household_demographics
    WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
        AND (store_sales.ss_store_sk = store.s_store_sk)
        AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
        AND ((household_demographics.hd_dep_count = 6) OR (household_demographics.hd_vehicle_count > 2))
        AND (date_dim.d_dow = 1)
        AND (date_dim.d_year IN (1999, 1999 + 1, 1999 + 2))
        AND ((store.s_number_employees >= 200) AND (store.s_number_employees <= 295))
    GROUP BY
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        store.s_city
) AS ms, customer
WHERE (ss_customer_sk = c_customer_sk)
ORDER BY
    c_last_name,
    c_first_name,
    substr(s_city, 1, 30),
    profit
LIMIT 100;
