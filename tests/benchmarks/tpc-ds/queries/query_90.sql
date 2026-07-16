SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM
(
    SELECT count(*) AS amc
    FROM web_sales, household_demographics, time_dim, web_page
    WHERE (ws_sold_time_sk = time_dim.t_time_sk)
        AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
        AND (ws_web_page_sk = web_page.wp_web_page_sk)
        AND (time_dim.t_hour BETWEEN 8 AND 8 + 1)
        AND (household_demographics.hd_dep_count = 6)
        AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
) AS at,
(
    SELECT count(*) AS pmc
    FROM web_sales, household_demographics, time_dim, web_page
    WHERE (ws_sold_time_sk = time_dim.t_time_sk)
        AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
        AND (ws_web_page_sk = web_page.wp_web_page_sk)
        AND (time_dim.t_hour BETWEEN 19 AND 19 + 1)
        AND (household_demographics.hd_dep_count = 6)
        AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
) AS pt
ORDER BY am_pm_ratio
LIMIT 100;
