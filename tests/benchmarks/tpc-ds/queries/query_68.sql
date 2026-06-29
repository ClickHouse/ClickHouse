SELECT
    c_last_name,
    c_first_name,
    ca_city,
    bought_city,
    ss_ticket_number,
    extended_price,
    extended_tax,
    list_price
FROM
(
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        ca_city AS bought_city,
        sum(ss_ext_sales_price) AS extended_price,
        sum(ss_ext_list_price) AS list_price,
        sum(ss_ext_tax) AS extended_tax
    FROM store_sales, date_dim, store, household_demographics, customer_address
    WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
        AND (store_sales.ss_store_sk = store.s_store_sk)
        AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
        AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
        AND ((date_dim.d_dom >= 1) AND (date_dim.d_dom <= 2))
        AND ((household_demographics.hd_dep_count = 4) OR (household_demographics.hd_vehicle_count = 3))
        AND (date_dim.d_year IN (1999, 1999 + 1, 1999 + 2))
        AND (store.s_city IN ('Midway', 'Fairview'))
    GROUP BY
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        ca_city
) AS dn, customer, customer_address AS current_addr
WHERE (ss_customer_sk = c_customer_sk)
    AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
    AND (current_addr.ca_city != bought_city)
ORDER BY
    c_last_name,
    ss_ticket_number
LIMIT 100;
