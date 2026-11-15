SELECT
    count(DISTINCT ws_order_number) AS "order count",
    sum(ws_ext_ship_cost) AS "total shipping cost",
    sum(ws_net_profit) AS "total net profit"
FROM web_sales AS ws1, date_dim, customer_address, web_site
WHERE (d_date BETWEEN '1999-2-01' AND (CAST('1999-2-01', 'date') + INTERVAL 60 DAY))
    AND (ws1.ws_ship_date_sk = d_date_sk)
    AND (ws1.ws_ship_addr_sk = ca_address_sk)
    AND (ca_state = 'IL')
    AND (ws1.ws_web_site_sk = web_site_sk)
    AND (web_company_name = 'pri')
    AND EXISTS (
        SELECT *
        FROM web_sales AS ws2
        WHERE (ws1.ws_order_number = ws2.ws_order_number)
            AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
    )
    AND NOT EXISTS (
        SELECT *
        FROM web_returns AS wr1
        WHERE (ws1.ws_order_number = wr1.wr_order_number)
    )
ORDER BY count(DISTINCT ws_order_number)
LIMIT 100;
