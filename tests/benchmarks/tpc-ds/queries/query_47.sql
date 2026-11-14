WITH
    v1 AS
    (
        SELECT
            i_category,
            i_brand,
            s_store_name,
            s_company_name,
            d_year,
            d_moy,
            sum(ss_sales_price) AS sum_sales,
            avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) AS avg_monthly_sales,
            rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year, d_moy) AS rn
        FROM item, store_sales, date_dim, store
        WHERE (ss_item_sk = i_item_sk)
            AND (ss_sold_date_sk = d_date_sk)
            AND (ss_store_sk = s_store_sk)
            AND (
                (d_year = 1999)
                OR ((d_year = 1999 - 1) AND (d_moy = 12))
                OR ((d_year = 1999 + 1) AND (d_moy = 1))
            )
        GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
    ),
    v2 AS
    (
        SELECT
            v1.i_category,
            v1.i_brand,
            v1.s_store_name,
            v1.s_company_name,
            v1.d_year,
            v1.d_moy,
            v1.avg_monthly_sales,
            v1.sum_sales,
            v1_lag.sum_sales AS psum,
            v1_lead.sum_sales AS nsum
        FROM v1, v1 AS v1_lag, v1 AS v1_lead
        WHERE (v1.i_category = v1_lag.i_category)
            AND (v1.i_category = v1_lead.i_category)
            AND (v1.i_brand = v1_lag.i_brand)
            AND (v1.i_brand = v1_lead.i_brand)
            AND (v1.s_store_name = v1_lag.s_store_name)
            AND (v1.s_store_name = v1_lead.s_store_name)
            AND (v1.s_company_name = v1_lag.s_company_name)
            AND (v1.s_company_name = v1_lead.s_company_name)
            AND (v1.rn = v1_lag.rn + 1)
            AND (v1.rn = v1_lead.rn - 1)
    )
SELECT *
FROM v2
WHERE (d_year = 1999)
    AND (avg_monthly_sales > 0)
    AND (CASE WHEN avg_monthly_sales > 0 THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1)
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100;
