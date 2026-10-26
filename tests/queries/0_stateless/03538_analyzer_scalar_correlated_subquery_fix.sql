set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

CREATE TABLE partsupp (
    ps_partkey     Int32,
    ps_suppkey     Int32,
    ps_availqty    Int32
)
ORDER BY (ps_partkey, ps_suppkey);

INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty) VALUES (114, 115, 1), (369, 7870, 1);

CREATE TABLE lineitem (
    l_partkey        Int32,
    l_suppkey        Int32,
    l_quantity       Decimal(15,2)
)
ORDER BY ();

INSERT INTO lineitem (l_partkey, l_suppkey, l_quantity) VALUES (1, 1, 1.0);

SELECT
    ps_partkey,
    ps_suppkey,
    (
        SELECT 0.5 * sum(l_quantity)
        FROM lineitem
        WHERE (l_partkey = ps_partkey) AND (l_suppkey = ps_suppkey)
    ) AS half_sum_quantity
FROM partsupp
WHERE (ps_partkey, ps_suppkey) IN ((114, 115), (369, 7870))
ORDER BY ps_partkey, ps_suppkey;

SELECT
    ps_partkey,
    ps_suppkey
FROM partsupp
WHERE ps_availqty > (
        SELECT 0.5 * sum(l_quantity)
        FROM lineitem
        WHERE (l_partkey = ps_partkey) AND (l_suppkey = ps_suppkey)
    );
