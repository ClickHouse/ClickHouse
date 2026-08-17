# List of known problems
## Q17
The query returns `nan` instead of `NULL` when `stddev_samp` is called on a single value. Corresponding issue: https://github.com/ClickHouse/ClickHouse/issues/94683. Otherwise, the result is correct.

## Q35
Memory Limit Exceeded with reasonable amount of memory.

## Q47
The query doesn't work out-of-the-box due to https://github.com/ClickHouse/ClickHouse/issues/94858. The alternative formulation with a minor fix works.

Original:
```sql
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
```

Alternative:
```sql
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
WHERE (v1.d_year = 1999)
    AND (v1.avg_monthly_sales > 0)
    AND (CASE WHEN v1.avg_monthly_sales > 0 THEN abs(v1.sum_sales - v1.avg_monthly_sales) / v1.avg_monthly_sales ELSE NULL END > 0.1)
ORDER BY v1.sum_sales - v1.avg_monthly_sales, v1.s_store_name
LIMIT 100;
```

## Q51
The result contains ᴺᵁᴸᴸ. This is correct, but we were using format_tsv_null_representation='' setting to represent nulls as empty strings. This is due to https://github.com/ClickHouse/ClickHouse/issues/95168. Note: running this query for the first time may produce '', but consecutive runs will produce 'ᴺᵁᴸᴸ'.

## Q57
The query doesn't work out-of-the-box due to https://github.com/ClickHouse/ClickHouse/issues/94858. The alternative formulation with a minor fix works.

Original:
```sql
WITH
    v1 AS
    (
        SELECT
            i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy,
            sum(cs_sales_price) AS sum_sales,
            avg(sum(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) AS avg_monthly_sales,
            rank() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS rn
        FROM item, catalog_sales, date_dim, call_center
        WHERE (cs_item_sk = i_item_sk)
            AND (cs_sold_date_sk = d_date_sk)
            AND (cc_call_center_sk = cs_call_center_sk)
            AND (
                (d_year = 1999)
                OR ((d_year = 1999 - 1) AND (d_moy = 12))
                OR ((d_year = 1999 + 1) AND (d_moy = 1))
            )
        GROUP BY i_category, i_brand, cc_name, d_year, d_moy
    ),
    v2 AS
    (
        SELECT
            v1.i_category,
            v1.i_brand,
            v1.cc_name,
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
            AND (v1.cc_name = v1_lag.cc_name)
            AND (v1.cc_name = v1_lead.cc_name)
            AND (v1.rn = v1_lag.rn + 1)
            AND (v1.rn = v1_lead.rn - 1)
    )
SELECT *
FROM v2
WHERE (d_year = 1999)
    AND (avg_monthly_sales > 0)
    AND (CASE WHEN avg_monthly_sales > 0 THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1)
ORDER BY sum_sales - avg_monthly_sales, cc_name
LIMIT 100;
```

Alternative:
```sql
WITH
    v1 AS
    (
        SELECT
            i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy,
            sum(cs_sales_price) AS sum_sales,
            avg(sum(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) AS avg_monthly_sales,
            rank() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS rn
        FROM item, catalog_sales, date_dim, call_center
        WHERE (cs_item_sk = i_item_sk)
            AND (cs_sold_date_sk = d_date_sk)
            AND (cc_call_center_sk = cs_call_center_sk)
            AND (
                (d_year = 1999)
                OR ((d_year = 1999 - 1) AND (d_moy = 12))
                OR ((d_year = 1999 + 1) AND (d_moy = 1))
            )
        GROUP BY i_category, i_brand, cc_name, d_year, d_moy
    ),
    v2 AS
    (
        SELECT
            v1.i_category,
            v1.i_brand,
            v1.cc_name,
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
            AND (v1.cc_name = v1_lag.cc_name)
            AND (v1.cc_name = v1_lead.cc_name)
            AND (v1.rn = v1_lag.rn + 1)
            AND (v1.rn = v1_lead.rn - 1)
    )
SELECT *
FROM v2
WHERE (v1.d_year = 1999)
    AND (v1.avg_monthly_sales > 0)
    AND (CASE WHEN v1.avg_monthly_sales > 0 THEN abs(v1.sum_sales - v1.avg_monthly_sales) / v1.avg_monthly_sales ELSE NULL END > 0.1)
ORDER BY v1.sum_sales - v1.avg_monthly_sales, v1.cc_name
LIMIT 100;
```

## Q58
The query doesn't work out-of-the-box due to https://github.com/ClickHouse/ClickHouse/issues/94976. The alternative formulation with a minor fix works.

Original:
```sql
WITH
    ss_items AS
    (
        SELECT
            i_item_id AS item_id,
            sum(ss_ext_sales_price) AS ss_item_rev
        FROM store_sales, item, date_dim
        WHERE (ss_item_sk = i_item_sk)
            AND (d_date IN (
                SELECT d_date
                FROM date_dim
                WHERE (d_week_seq = (
                    SELECT d_week_seq
                    FROM date_dim
                    WHERE (d_date = '2000-01-03')
                ))
            ))
            AND (ss_sold_date_sk = d_date_sk)
        GROUP BY i_item_id
    ),
    cs_items AS
    (
        SELECT
            i_item_id AS item_id,
            sum(cs_ext_sales_price) AS cs_item_rev
        FROM catalog_sales, item, date_dim
        WHERE (cs_item_sk = i_item_sk)
            AND (d_date IN (
                SELECT d_date
                FROM date_dim
                WHERE (d_week_seq = (
                    SELECT d_week_seq
                    FROM date_dim
                    WHERE (d_date = '2000-01-03')
                ))
            ))
            AND (cs_sold_date_sk = d_date_sk)
        GROUP BY i_item_id
    ),
    ws_items AS
    (
        SELECT
            i_item_id AS item_id,
            sum(ws_ext_sales_price) AS ws_item_rev
        FROM web_sales, item, date_dim
        WHERE (ws_item_sk = i_item_sk)
            AND (d_date IN (
                SELECT d_date
                FROM date_dim
                WHERE (d_week_seq = (
                    SELECT d_week_seq
                    FROM date_dim
                    WHERE (d_date = '2000-01-03')
                ))
            ))
            AND (ws_sold_date_sk = d_date_sk)
        GROUP BY i_item_id
    )
SELECT
    ss_items.item_id,
    ss_item_rev,
    ss_item_rev / ((CAST(ss_item_rev AS Float64) + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
    cs_item_rev,
    cs_item_rev / ((CAST(ss_item_rev AS Float64) + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
    ws_item_rev,
    ws_item_rev / ((CAST(ss_item_rev AS Float64) + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM ss_items, cs_items, ws_items
WHERE (ss_items.item_id = cs_items.item_id)
    AND (ss_items.item_id = ws_items.item_id)
    AND (ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev)
    AND (ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev)
    AND (cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev)
    AND (cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev)
    AND (ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev)
    AND (ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev)
ORDER BY
    item_id,
    ss_item_rev
LIMIT 100;
```

Alternative:
```sql
WITH
    ss_items AS
    (
        SELECT
            i_item_id AS item_id,
            sum(ss_ext_sales_price) AS ss_item_rev
        FROM store_sales, item, date_dim
        WHERE (ss_item_sk = i_item_sk)
            AND (d_date IN (
                SELECT d_date
                FROM date_dim
                WHERE (d_week_seq = (
                    SELECT d_week_seq
                    FROM date_dim
                    WHERE (d_date = '2000-01-03')
                ))
            ))
            AND (ss_sold_date_sk = d_date_sk)
        GROUP BY i_item_id
    ),
    cs_items AS
    (
        SELECT
            i_item_id AS item_id,
            sum(cs_ext_sales_price) AS cs_item_rev
        FROM catalog_sales, item, date_dim
        WHERE (cs_item_sk = i_item_sk)
            AND (d_date IN (
                SELECT d_date
                FROM date_dim
                WHERE (d_week_seq = (
                    SELECT d_week_seq
                    FROM date_dim
                    WHERE (d_date = '2000-01-03')
                ))
            ))
            AND (cs_sold_date_sk = d_date_sk)
        GROUP BY i_item_id
    ),
    ws_items AS
    (
        SELECT
            i_item_id AS item_id,
            sum(ws_ext_sales_price) AS ws_item_rev
        FROM web_sales, item, date_dim
        WHERE (ws_item_sk = i_item_sk)
            AND (d_date IN (
                SELECT d_date
                FROM date_dim
                WHERE (d_week_seq = (
                    SELECT d_week_seq
                    FROM date_dim
                    WHERE (d_date = '2000-01-03')
                ))
            ))
            AND (ws_sold_date_sk = d_date_sk)
        GROUP BY i_item_id
    )
SELECT
    ss_items.item_id,
    ss_item_rev,
    ss_item_rev / ((CAST(ss_item_rev AS Float64) + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
    cs_item_rev,
    cs_item_rev / ((CAST(ss_item_rev AS Float64) + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
    ws_item_rev,
    ws_item_rev / ((CAST(ss_item_rev AS Float64) + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM ss_items, cs_items, ws_items
WHERE (ss_items.item_id = cs_items.item_id)
    AND (ss_items.item_id = ws_items.item_id)
    AND (ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev)
    AND (ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev)
    AND (cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev)
    AND (cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev)
    AND (ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev)
    AND (ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev)
ORDER BY
    ss_items.item_id,
    ss_item_rev
LIMIT 100;
```

## Q64-65
The result contains ᴺᵁᴸᴸ. This is correct, but we were using format_tsv_null_representation='' setting to represent nulls as empty strings. This is due to https://github.com/ClickHouse/ClickHouse/issues/95168. Note: running this query for the first time may produce '', but consecutive runs will produce 'ᴺᵁᴸᴸ'.


## Q75
The query doesn't work out-of-the-box due to https://github.com/ClickHouse/ClickHouse/issues/94671. The alternative formulation with a minor fix works.

Original:
```sql
WITH
    all_sales AS
    (
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            SUM(sales_cnt) AS sales_cnt,
            SUM(sales_amt) AS sales_amt
        FROM
        (
            SELECT
                d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
                cs_ext_sales_price - COALESCE(cr_return_amount, 0.) AS sales_amt
            FROM catalog_sales
            INNER JOIN item ON i_item_sk = cs_item_sk
            INNER JOIN date_dim ON d_date_sk = cs_sold_date_sk
            LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number) AND (cs_item_sk = cr_item_sk)
            WHERE (i_category = 'Books')
            UNION
            SELECT
                d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                ss_ext_sales_price - COALESCE(sr_return_amt, 0.) AS sales_amt
            FROM store_sales
            INNER JOIN item ON i_item_sk = ss_item_sk
            INNER JOIN date_dim ON d_date_sk = ss_sold_date_sk
            LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number) AND (ss_item_sk = sr_item_sk)
            WHERE (i_category = 'Books')
            UNION
            SELECT
                d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                ws_ext_sales_price - COALESCE(wr_return_amt, 0.) AS sales_amt
            FROM web_sales
            INNER JOIN item ON i_item_sk = ws_item_sk
            INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk
            LEFT JOIN web_returns ON (ws_order_number = wr_order_number) AND (ws_item_sk = wr_item_sk)
            WHERE (i_category = 'Books')
        ) AS sales_detail
        GROUP BY
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id
    )
SELECT
    prev_yr.d_year AS prev_year,
    curr_yr.d_year AS year,
    curr_yr.i_brand_id,
    curr_yr.i_class_id,
    curr_yr.i_category_id,
    curr_yr.i_manufact_id,
    prev_yr.sales_cnt AS prev_yr_cnt,
    curr_yr.sales_cnt AS curr_yr_cnt,
    curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
    curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM all_sales AS curr_yr, all_sales AS prev_yr
WHERE (curr_yr.i_brand_id = prev_yr.i_brand_id)
    AND (curr_yr.i_class_id = prev_yr.i_class_id)
    AND (curr_yr.i_category_id = prev_yr.i_category_id)
    AND (curr_yr.i_manufact_id = prev_yr.i_manufact_id)
    AND (curr_yr.d_year = 2002)
    AND (prev_yr.d_year = (2002 - 1))
    AND ((CAST(curr_yr.sales_cnt, 'DECIMAL(17, 2)') / CAST(prev_yr.sales_cnt, 'DECIMAL(17, 2)')) < 0.9)
ORDER BY
    sales_cnt_diff,
    sales_amt_diff
LIMIT 100;
```

Alternative:
```sql
WITH
    all_sales AS
    (
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            SUM(sales_cnt) AS sales_cnt,
            SUM(sales_amt) AS sales_amt
        FROM
        (
            SELECT
                d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
                cs_ext_sales_price - COALESCE(cr_return_amount, CAST('0.0', 'Decimal(7, 2)')) AS sales_amt
            FROM catalog_sales
            INNER JOIN item ON i_item_sk = cs_item_sk
            INNER JOIN date_dim ON d_date_sk = cs_sold_date_sk
            LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number) AND (cs_item_sk = cr_item_sk)
            WHERE (i_category = 'Books')
            UNION
            SELECT
                d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                ss_ext_sales_price - COALESCE(sr_return_amt, CAST('0.0', 'Decimal(7, 2)')) AS sales_amt
            FROM store_sales
            INNER JOIN item ON i_item_sk = ss_item_sk
            INNER JOIN date_dim ON d_date_sk = ss_sold_date_sk
            LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number) AND (ss_item_sk = sr_item_sk)
            WHERE (i_category = 'Books')
            UNION
            SELECT
                d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                ws_ext_sales_price - COALESCE(wr_return_amt, CAST('0.0', 'Decimal(7, 2)')) AS sales_amt
            FROM web_sales
            INNER JOIN item ON i_item_sk = ws_item_sk
            INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk
            LEFT JOIN web_returns ON (ws_order_number = wr_order_number) AND (ws_item_sk = wr_item_sk)
            WHERE (i_category = 'Books')
        ) AS sales_detail
        GROUP BY
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id
    )
SELECT
    prev_yr.d_year AS prev_year,
    curr_yr.d_year AS year,
    curr_yr.i_brand_id,
    curr_yr.i_class_id,
    curr_yr.i_category_id,
    curr_yr.i_manufact_id,
    prev_yr.sales_cnt AS prev_yr_cnt,
    curr_yr.sales_cnt AS curr_yr_cnt,
    curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
    curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM all_sales AS curr_yr, all_sales AS prev_yr
WHERE (curr_yr.i_brand_id = prev_yr.i_brand_id)
    AND (curr_yr.i_class_id = prev_yr.i_class_id)
    AND (curr_yr.i_category_id = prev_yr.i_category_id)
    AND (curr_yr.i_manufact_id = prev_yr.i_manufact_id)
    AND (curr_yr.d_year = 2002)
    AND (prev_yr.d_year = (2002 - 1))
    AND ((CAST(curr_yr.sales_cnt, 'DECIMAL(17, 2)') / CAST(prev_yr.sales_cnt, 'DECIMAL(17, 2)')) < 0.9)
ORDER BY
    sales_cnt_diff,
    sales_amt_diff
LIMIT 100;
```
