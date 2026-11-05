-- https://github.com/ClickHouse/ClickHouse/issues/75604
-- Distributed table with distributed_group_by_no_merge=1 inside a Merge table
-- should not throw LOGICAL_ERROR.

DROP TABLE IF EXISTS local_table;
DROP TABLE IF EXISTS dist_table;
DROP TABLE IF EXISTS merge_table;
DROP TABLE IF EXISTS second_local;
DROP TABLE IF EXISTS second_dist;

CREATE TABLE local_table (id String, id2 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_table (id String, id2 String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_table');
CREATE TABLE merge_table (id String, id2 String) ENGINE = Merge(currentDatabase(), '^(dist_table)$');

CREATE TABLE second_local (id String, id2 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE second_dist (id String, id2 String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'second_local');

INSERT INTO local_table VALUES ('foo', 'bar');
INSERT INTO second_local VALUES ('foo', 'bar');

SET distributed_group_by_no_merge = 1;

SELECT count() FROM merge_table AS s GLOBAL ANY INNER JOIN second_dist AS f USING (id) WHERE f.id2 GLOBAL IN (SELECT id2 FROM second_dist GROUP BY id2);

DROP TABLE merge_table;
DROP TABLE dist_table;
DROP TABLE local_table;
DROP TABLE second_dist;
DROP TABLE second_local;
