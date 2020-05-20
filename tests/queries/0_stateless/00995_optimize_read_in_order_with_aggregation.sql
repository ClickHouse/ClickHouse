SET optimize_read_in_order = 1;
DROP TABLE IF EXISTS order_with_aggr;
CREATE TABLE order_with_aggr(a Int) ENGINE = MergeTree ORDER BY a;

INSERT INTO order_with_aggr SELECT * FROM numbers(100);
SELECT sum(a) as s FROM order_with_aggr ORDER BY s;

DROP TABLE order_with_aggr;
