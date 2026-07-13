WITH ws_wh AS
    (
        SELECT
            ws1.ws_order_number
        FROM
        (
            SELECT
                1 AS ws_order_number,
                1 AS ws_warehouse_sk
        ) AS ws1,
        (
            SELECT
                1 AS ws_order_number,
                2 AS ws_warehouse_sk
        ) AS ws2
        WHERE (ws1.ws_order_number = ws2.ws_order_number) AND (ws1.ws_warehouse_sk != ws2.ws_warehouse_sk)
    )
SELECT COUNT()
FROM
(
    SELECT 1 AS ws_order_number
) AS ws1
WHERE (ws1.ws_order_number IN (
    SELECT ws_order_number
    FROM ws_wh
))
SETTINGS join_use_nulls=1;
