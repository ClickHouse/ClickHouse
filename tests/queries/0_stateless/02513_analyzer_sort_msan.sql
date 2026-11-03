DROP TABLE IF EXISTS products;

CREATE TABLE products (`price` UInt32) ENGINE = Memory;
INSERT INTO products VALUES (1);

SELECT rank() OVER (ORDER BY price) AS rank FROM products ORDER BY rank;
