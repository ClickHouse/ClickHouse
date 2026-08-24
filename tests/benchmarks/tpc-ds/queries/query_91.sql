SELECT
    cc_call_center_id AS Call_Center,
    cc_name AS Call_Center_Name,
    cc_manager AS Manager,
    sum(cr_net_loss) AS Returns_Loss
FROM call_center, catalog_returns, date_dim, customer, customer_address, customer_demographics, household_demographics
WHERE (cr_call_center_sk = cc_call_center_sk)
    AND (cr_returned_date_sk = d_date_sk)
    AND (cr_returning_customer_sk = c_customer_sk)
    AND (cd_demo_sk = c_current_cdemo_sk)
    AND (hd_demo_sk = c_current_hdemo_sk)
    AND (ca_address_sk = c_current_addr_sk)
    AND (d_year = 1998)
    AND (d_moy = 11)
    AND (
        ((cd_marital_status = 'M') AND (cd_education_status = 'Unknown'))
        OR ((cd_marital_status = 'W') AND (cd_education_status = 'Advanced Degree'))
    )
    AND (hd_buy_potential LIKE 'Unknown%')
    AND (ca_gmt_offset = -7)
GROUP BY
    cc_call_center_id,
    cc_name,
    cc_manager,
    cd_marital_status,
    cd_education_status
ORDER BY sum(cr_net_loss) DESC;
