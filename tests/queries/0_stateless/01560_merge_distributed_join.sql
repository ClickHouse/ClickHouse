-- test from https://github.com/ClickHouse/ClickHouse/issues/11755#issuecomment-700850254
DROP TABLE IF EXISTS cat_hist;
DROP TABLE IF EXISTS prod_hist;
DROP TABLE IF EXISTS products_l;
DROP TABLE IF EXISTS products;

CREATE TABLE cat_hist (categoryId UUID, categoryName String) ENGINE Memory;
CREATE TABLE prod_hist (categoryId UUID, productId UUID) ENGINE = MergeTree ORDER BY productId;

CREATE TABLE products_l AS prod_hist ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), prod_hist);
CREATE TABLE products as prod_hist ENGINE = Merge(currentDatabase(), '^products_');

SELECT * FROM products AS p LEFT JOIN cat_hist AS c USING (categoryId);
SELECT * FROM products AS p GLOBAL LEFT JOIN cat_hist AS c USING (categoryId);

DROP TABLE cat_hist;
DROP TABLE prod_hist;
DROP TABLE products_l;
DROP TABLE products;
