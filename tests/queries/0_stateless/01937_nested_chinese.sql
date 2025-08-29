-- Disable this setting to properly test String type
SET data_type_string_use_size_stream = 0;

CREATE TEMPORARY TABLE test (`id` String, `products` Nested (`产品` Array(String), `销量` Array(Int32)));

DESCRIBE test;
DESCRIBE (SELECT * FROM test);
DESCRIBE (SELECT * FROM test ARRAY JOIN products);
DESCRIBE (SELECT p.`产品`, p.`销量` FROM test ARRAY JOIN products AS p);
SELECT * FROM test ARRAY JOIN products;
SELECT count() FROM (SELECT * FROM test ARRAY JOIN products);
