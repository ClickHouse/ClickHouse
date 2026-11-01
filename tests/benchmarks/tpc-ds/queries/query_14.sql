WITH
    cross_items AS
    (
        SELECT i_item_sk AS ss_item_sk
        FROM item,
        (
            SELECT
                iss.i_brand_id AS brand_id,
                iss.i_class_id AS class_id,
                iss.i_category_id AS category_id
            FROM store_sales, item AS iss, date_dim AS d1
            WHERE (ss_item_sk = iss.i_item_sk) AND (ss_sold_date_sk = d1.d_date_sk) AND (d1.d_year BETWEEN 1999 AND 1999 + 2)
            INTERSECT
            SELECT
                ics.i_brand_id,
                ics.i_class_id,
                ics.i_category_id
            FROM catalog_sales, item AS ics, date_dim AS d2
            WHERE (cs_item_sk = ics.i_item_sk) AND (cs_sold_date_sk = d2.d_date_sk) AND (d2.d_year BETWEEN 1999 AND 1999 + 2)
            INTERSECT
            SELECT
                iws.i_brand_id,
                iws.i_class_id,
                iws.i_category_id
            FROM web_sales, item AS iws, date_dim AS d3
            WHERE (ws_item_sk = iws.i_item_sk) AND (ws_sold_date_sk = d3.d_date_sk) AND (d3.d_year BETWEEN 1999 AND 1999 + 2)
        )
        WHERE (i_brand_id = brand_id) AND (i_class_id = class_id) AND (i_category_id = category_id)
    ),
    avg_sales AS
    (
        SELECT avg(quantity * list_price) AS average_sales
        FROM
        (
            SELECT
                ss_quantity AS quantity,
                ss_list_price AS list_price
            FROM store_sales, date_dim
            WHERE (ss_sold_date_sk = d_date_sk) AND (d_year BETWEEN 1999 AND 1999 + 2)
            UNION ALL
            SELECT
                cs_quantity AS quantity,
                cs_list_price AS list_price
            FROM catalog_sales, date_dim
            WHERE (cs_sold_date_sk = d_date_sk) AND (d_year BETWEEN 1999 AND 1999 + 2)
            UNION ALL
            SELECT
                ws_quantity AS quantity,
                ws_list_price AS list_price
            FROM web_sales, date_dim
            WHERE (ws_sold_date_sk = d_date_sk) AND (d_year BETWEEN 1999 AND 1999 + 2)
        ) AS x
    )
SELECT
    channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    sum(sales),
    sum(number_sales)
FROM
(
    SELECT
        'store' AS channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sum(ss_quantity * ss_list_price) AS sales,
        count(*) AS number_sales
    FROM store_sales, item, date_dim
    WHERE (ss_item_sk IN (
        SELECT ss_item_sk
        FROM cross_items
    )) AND (ss_item_sk = i_item_sk) AND (ss_sold_date_sk = d_date_sk) AND (d_year = (1999 + 2)) AND (d_moy = 11)
    GROUP BY
        i_brand_id,
        i_class_id,
        i_category_id
    HAVING sum(ss_quantity * ss_list_price) > (
        SELECT average_sales
        FROM avg_sales
    )
    UNION ALL
    SELECT
        'catalog' AS channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sum(cs_quantity * cs_list_price) AS sales,
        count(*) AS number_sales
    FROM catalog_sales, item, date_dim
    WHERE (cs_item_sk IN (
        SELECT ss_item_sk
        FROM cross_items
    )) AND (cs_item_sk = i_item_sk) AND (cs_sold_date_sk = d_date_sk) AND (d_year = (1999 + 2)) AND (d_moy = 11)
    GROUP BY
        i_brand_id,
        i_class_id,
        i_category_id
    HAVING sum(cs_quantity * cs_list_price) > (
        SELECT average_sales
        FROM avg_sales
    )
    UNION ALL
    SELECT
        'web' AS channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sum(ws_quantity * ws_list_price) AS sales,
        count(*) AS number_sales
    FROM web_sales, item, date_dim
    WHERE (ws_item_sk IN (
        SELECT ss_item_sk
        FROM cross_items
    )) AND (ws_item_sk = i_item_sk) AND (ws_sold_date_sk = d_date_sk) AND (d_year = (1999 + 2)) AND (d_moy = 11)
    GROUP BY
        i_brand_id,
        i_class_id,
        i_category_id
    HAVING sum(ws_quantity * ws_list_price) > (
        SELECT average_sales
        FROM avg_sales
    )
) AS y
GROUP BY
    channel,
    i_brand_id,
    i_class_id,
    i_category_id
    WITH ROLLUP
ORDER BY
    channel,
    i_brand_id,
    i_class_id,
    i_category_id
LIMIT 100;


WITH
    cross_items AS
    (
        SELECT i_item_sk AS ss_item_sk
        FROM item,
        (
            SELECT
                iss.i_brand_id AS brand_id,
                iss.i_class_id AS class_id,
                iss.i_category_id AS category_id
            FROM store_sales, item AS iss, date_dim AS d1
            WHERE (ss_item_sk = iss.i_item_sk) AND (ss_sold_date_sk = d1.d_date_sk) AND (d1.d_year BETWEEN 1999 AND 1999 + 2)
            INTERSECT
            SELECT
                ics.i_brand_id,
                ics.i_class_id,
                ics.i_category_id
            FROM catalog_sales, item AS ics, date_dim AS d2
            WHERE (cs_item_sk = ics.i_item_sk) AND (cs_sold_date_sk = d2.d_date_sk) AND (d2.d_year BETWEEN 1999 AND 1999 + 2)
            INTERSECT
            SELECT
                iws.i_brand_id,
                iws.i_class_id,
                iws.i_category_id
            FROM web_sales, item AS iws, date_dim AS d3
            WHERE (ws_item_sk = iws.i_item_sk) AND (ws_sold_date_sk = d3.d_date_sk) AND (d3.d_year BETWEEN 1999 AND 1999 + 2)
        )
        WHERE (i_brand_id = brand_id) AND (i_class_id = class_id) AND (i_category_id = category_id)
    ),
    avg_sales AS
    (
        SELECT avg(quantity * list_price) AS average_sales
        FROM
        (
            SELECT
                ss_quantity AS quantity,
                ss_list_price AS list_price
            FROM store_sales, date_dim
            WHERE (ss_sold_date_sk = d_date_sk) AND (d_year BETWEEN 1999 AND 1999 + 2)
            UNION ALL
            SELECT
                cs_quantity AS quantity,
                cs_list_price AS list_price
            FROM catalog_sales, date_dim
            WHERE (cs_sold_date_sk = d_date_sk) AND (d_year BETWEEN 1999 AND 1999 + 2)
            UNION ALL
            SELECT
                ws_quantity AS quantity,
                ws_list_price AS list_price
            FROM web_sales, date_dim
            WHERE (ws_sold_date_sk = d_date_sk) AND (d_year BETWEEN 1999 AND 1999 + 2)
        ) AS x
    )
SELECT
    this_year.channel AS ty_channel,
    this_year.i_brand_id AS ty_brand,
    this_year.i_class_id AS ty_class,
    this_year.i_category_id AS ty_category,
    this_year.sales AS ty_sales,
    this_year.number_sales AS ty_number_sales,
    last_year.channel AS ly_channel,
    last_year.i_brand_id AS ly_brand,
    last_year.i_class_id AS ly_class,
    last_year.i_category_id AS ly_category,
    last_year.sales AS ly_sales,
    last_year.number_sales AS ly_number_sales
FROM
(
    SELECT
        'store' AS channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sum(ss_quantity * ss_list_price) AS sales,
        count(*) AS number_sales
    FROM store_sales, item, date_dim
    WHERE (ss_item_sk IN (
        SELECT ss_item_sk
        FROM cross_items
    )) AND (ss_item_sk = i_item_sk) AND (ss_sold_date_sk = d_date_sk) AND (d_week_seq = (
        SELECT d_week_seq
        FROM date_dim
        WHERE (d_year = 1999 + 1) AND (d_moy = 12) AND (d_dom = 11)
    ))
    GROUP BY
        i_brand_id,
        i_class_id,
        i_category_id
    HAVING sum(ss_quantity * ss_list_price) > (
        SELECT average_sales
        FROM avg_sales
    )
) AS this_year,
(
    SELECT
        'store' AS channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sum(ss_quantity * ss_list_price) AS sales,
        count(*) AS number_sales
    FROM store_sales, item, date_dim
    WHERE (ss_item_sk IN (
        SELECT ss_item_sk
        FROM cross_items
    )) AND (ss_item_sk = i_item_sk) AND (ss_sold_date_sk = d_date_sk) AND (d_week_seq = (
        SELECT d_week_seq
        FROM date_dim
        WHERE (d_year = 1999) AND (d_moy = 12) AND (d_dom = 11)
    ))
    GROUP BY
        i_brand_id,
        i_class_id,
        i_category_id
    HAVING sum(ss_quantity * ss_list_price) > (
        SELECT average_sales
        FROM avg_sales
    )
) AS last_year
WHERE (this_year.i_brand_id = last_year.i_brand_id)
    AND (this_year.i_class_id = last_year.i_class_id)
    AND (this_year.i_category_id = last_year.i_category_id)
ORDER BY
    this_year.channel,
    this_year.i_brand_id,
    this_year.i_class_id,
    this_year.i_category_id
LIMIT 100;

