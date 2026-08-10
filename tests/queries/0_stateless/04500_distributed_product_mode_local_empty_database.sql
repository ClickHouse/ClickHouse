-- Tags: distributed

DROP TABLE IF EXISTS larger_table;
DROP TABLE IF EXISTS smaller_table;
DROP TABLE IF EXISTS dist_larger;
DROP TABLE IF EXISTS dist_smaller;

CREATE TABLE larger_table (key String) ENGINE = Memory;
CREATE TABLE smaller_table (key String) ENGINE = Memory;
CREATE TABLE dist_larger (key String) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'larger_table');
CREATE TABLE dist_smaller (key String) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'smaller_table');

EXPLAIN AST optimize = 1
SELECT count() FROM dist_larger AS lt INNER JOIN dist_smaller AS rt ON lt.key = rt.key
SETTINGS distributed_product_mode = 'local', enable_analyzer = 0;

DROP TABLE dist_larger;
DROP TABLE dist_smaller;
DROP TABLE larger_table;
DROP TABLE smaller_table;
