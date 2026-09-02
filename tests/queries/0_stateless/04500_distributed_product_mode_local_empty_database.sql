-- Tags: distributed

DROP TABLE IF EXISTS larger_table;
DROP TABLE IF EXISTS smaller_table;
DROP TABLE IF EXISTS dist_larger;
DROP TABLE IF EXISTS dist_smaller;

CREATE TABLE larger_table (key String) ENGINE = Memory;
CREATE TABLE smaller_table (key String) ENGINE = Memory;
CREATE TABLE dist_larger (key String) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'larger_table');
CREATE TABLE dist_smaller (key String) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'smaller_table');


DROP TABLE dist_larger;
DROP TABLE dist_smaller;
DROP TABLE larger_table;
DROP TABLE smaller_table;
