WITH
    customer_total_return AS
    (
        SELECT
            wr_returning_customer_sk AS ctr_customer_sk,
            ca_state AS ctr_state,
            sum(wr_return_amt) AS ctr_total_return
        FROM web_returns, date_dim, customer_address
        WHERE (wr_returned_date_sk = d_date_sk)
            AND (d_year = 2002)
            AND (wr_returning_addr_sk = ca_address_sk)
        GROUP BY
            wr_returning_customer_sk,
            ca_state
    )
SELECT
    c_customer_id,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date_sk,
    ctr_total_return
FROM customer_total_return AS ctr1, customer_address, customer
WHERE (ctr1.ctr_total_return > (
    SELECT avg(ctr_total_return) * 1.2
    FROM customer_total_return AS ctr2
    WHERE (ctr1.ctr_state = ctr2.ctr_state)
))
    AND (ca_address_sk = c_current_addr_sk)
    AND (ca_state = 'GA')
    AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY
    c_customer_id,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date_sk,
    ctr_total_return
LIMIT 100;
