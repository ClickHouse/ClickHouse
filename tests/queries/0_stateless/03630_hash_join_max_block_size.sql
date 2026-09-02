DROP TABLE IF EXISTS b_customers;
CREATE TABLE b_customers (customer_id Int64, first_order_id Int64) ENGINE = MergeTree ORDER BY customer_id;
INSERT INTO b_customers SELECT number, number * 20000000 FROM system.numbers LIMIT 2,100000; -- will work with LIMIT 10000
INSERT INTO b_customers SELECT number * -1, -1 FROM system.numbers LIMIT 2; -- will work without this line or LIMIT 1

DROP TABLE IF EXISTS b_orders;
CREATE TABLE b_orders (order_id Int64, address_id String) ENGINE = MergeTree ORDER BY order_id;
INSERT INTO b_orders SELECT number, 'fake' FROM system.numbers LIMIT 80000; -- will work with LIMIT 70000
INSERT INTO b_orders SELECT first_order_id, 'fake' FROM b_customers GROUP BY first_order_id;

DROP TABLE IF EXISTS b_addresses;
CREATE TABLE b_addresses (address_id String) ENGINE = MergeTree ORDER BY address_id;

set query_plan_join_swap_table = 0;

select count() from
(
  SELECT customers.customer_id AS dim_customers_id
  FROM
      b_orders AS orders
    ANY RIGHT JOIN
      b_customers AS customers
    ON orders.order_id = customers.first_order_id
    ANY LEFT JOIN
      b_addresses AS shipping_addresses
    ON shipping_addresses.address_id = orders.address_id
);
