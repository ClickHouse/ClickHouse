-- Regression test when Join stores data on disk and receive empty block.
-- Because of this it does not create empty file, while expect it.

SET max_threads = 1;
SET join_algorithm = 'auto';
SET max_rows_in_join = 1000;
SET optimize_aggregation_in_order = 1;
SET max_block_size = 1000;

DROP TABLE IF EXISTS join_on_disk;

SYSTEM STOP MERGES join_on_disk;

CREATE TABLE join_on_disk (id Int) Engine=MergeTree() ORDER BY id;

INSERT INTO join_on_disk SELECT number as id FROM numbers_mt(50000);
INSERT INTO join_on_disk SELECT number as id FROM numbers_mt(1000);

SELECT id FROM join_on_disk lhs LEFT JOIN (SELECT id FROM join_on_disk GROUP BY id) rhs USING (id) FORMAT Null;

DROP TABLE join_on_disk;
