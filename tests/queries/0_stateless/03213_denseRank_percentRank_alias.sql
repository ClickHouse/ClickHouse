-- https://github.com/ClickHouse/ClickHouse/issues/67042
-- Reference generated using percent_rank() and dense_rank()

-- From ClickHouse/tests/queries/0_stateless/01591_window_functions.sql  (for deterministic query)
SELECT '---- denseRank() ----';
select number, p, o,
    count(*) over w,
    rank() over w,
    denseRank() over w,
    row_number() over w
from (select number, intDiv(number, 5) p, mod(number, 3) o
    from numbers(31) order by o, number) t
window w as (partition by p order by o, number)
order by p, o, number
settings max_block_size = 2;

-- Modifed from ClickHouse/tests/queries/0_stateless/01592_window_functions.sql (for deterministic query)
SELECT '---- percentRank() ----';

drop table if exists product_groups;
drop table if exists products;

CREATE TABLE product_groups (
	group_id Int64,
	group_name String
) Engine = Memory;

CREATE TABLE products (
	product_id Int64,
	product_name String,
	price DECIMAL(11, 2),
	group_id Int64
) Engine = Memory;

INSERT INTO product_groups  VALUES	(1, 'Smartphone'),(2, 'Laptop'),(3, 'Tablet');
INSERT INTO products (product_id,product_name, group_id,price) VALUES (1, 'Microsoft Lumia', 1, 200), (2, 'HTC One', 1, 400), (3, 'Nexus', 1, 500), (4, 'iPhone', 1, 900),(5, 'HP Elite', 2, 1200),(6, 'Lenovo Thinkpad', 2, 700),(7, 'Sony VAIO', 2, 700),(8, 'Dell Vostro', 2, 800),(9, 'iPad', 3, 700),(10, 'Kindle Fire', 3, 150),(11, 'Samsung Galaxy Tab', 3, 200);
INSERT INTO product_groups  VALUES	(4, 'Unknow');
INSERT INTO products (product_id,product_name, group_id,price) VALUES (12, 'Others', 4, 200);


SELECT *
FROM
(
    SELECT
        product_name,
        group_name,
        price,
        rank() OVER (PARTITION BY group_name ORDER BY price ASC) AS rank,
        percentRank() OVER (PARTITION BY group_name ORDER BY price ASC) AS percent
    FROM products
    INNER JOIN product_groups USING (group_id)
) AS t
ORDER BY
    group_name ASC,
    price ASC,
    product_name ASC;

drop table product_groups;
drop table products;
