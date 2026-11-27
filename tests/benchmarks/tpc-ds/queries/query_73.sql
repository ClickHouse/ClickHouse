SELECT
    c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag,
    ss_ticket_number,
    cnt
FROM
(
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        count(*) AS cnt
    FROM store_sales, date_dim, store, household_demographics
    WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
        AND (store_sales.ss_store_sk = store.s_store_sk)
        AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
        AND ((date_dim.d_dom >= 1) AND (date_dim.d_dom <= 2))
        AND ((household_demographics.hd_buy_potential = '>10000') OR (household_demographics.hd_buy_potential = 'Unknown'))
        AND (household_demographics.hd_vehicle_count > 0)
        AND (multiIf(household_demographics.hd_vehicle_count > 0, household_demographics.hd_dep_count / household_demographics.hd_vehicle_count, NULL) > 1)
        AND (date_dim.d_year IN (1999, 1999 + 1, 1999 + 2))
        AND (store.s_county IN ('Williamson County', 'Franklin Parish', 'Bronx County', 'Orange County'))
    GROUP BY
        ss_ticket_number,
        ss_customer_sk
) AS dj, customer
WHERE (ss_customer_sk = c_customer_sk)
    AND ((cnt >= 1) AND (cnt <= 5))
ORDER BY
    cnt DESC,
    c_last_name;
