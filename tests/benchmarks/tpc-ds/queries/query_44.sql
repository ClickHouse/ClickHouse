SELECT
    asceding.rnk,
    i1.i_product_name AS best_performing,
    i2.i_product_name AS worst_performing
FROM
(
    SELECT *
    FROM
    (
        SELECT
            item_sk,
            rank() OVER (ORDER BY rank_col ASC) AS rnk
        FROM
        (
            SELECT
                ss_item_sk AS item_sk,
                avg(ss_net_profit) AS rank_col
            FROM store_sales AS ss1
            WHERE (ss_store_sk = 4)
            GROUP BY ss_item_sk
            HAVING avg(ss_net_profit) > 0.9 * (
                SELECT avg(ss_net_profit) AS rank_col
                FROM store_sales
                WHERE (ss_store_sk = 4) AND (ss_addr_sk IS NULL)
                GROUP BY ss_store_sk
            )
        ) AS V1
    ) AS V11
    WHERE (rnk < 11)
) AS asceding,
(
    SELECT *
    FROM
    (
        SELECT
            item_sk,
            rank() OVER (ORDER BY rank_col DESC) AS rnk
        FROM
        (
            SELECT
                ss_item_sk AS item_sk,
                avg(ss_net_profit) AS rank_col
            FROM store_sales AS ss1
            WHERE (ss_store_sk = 4)
            GROUP BY ss_item_sk
            HAVING avg(ss_net_profit) > 0.9 * (
                SELECT avg(ss_net_profit) AS rank_col
                FROM store_sales
                WHERE (ss_store_sk = 4) AND (ss_addr_sk IS NULL)
                GROUP BY ss_store_sk
            )
        ) AS V2
    ) AS V21
    WHERE (rnk < 11)
) AS descending,
item AS i1,
item AS i2
WHERE (asceding.rnk = descending.rnk)
    AND (i1.i_item_sk = asceding.item_sk)
    AND (i2.i_item_sk = descending.item_sk)
ORDER BY asceding.rnk
LIMIT 100;
