DROP TABLE IF EXISTS pk;

CREATE TABLE pk (d Date DEFAULT '2000-01-01', x UInt64, y UInt64, z UInt64) ENGINE = MergeTree(d, (x, y, z), 1);

INSERT INTO pk (x, y, z) VALUES (1, 11, 1235), (1, 11, 4395), (1, 22, 3545), (1, 22, 6984), (1, 33, 4596), (2, 11, 4563), (2, 11, 4578), (2, 11, 3572), (2, 22, 5786), (2, 22, 5786), (2, 22, 2791), (2, 22, 2791), (3, 33, 2791), (3, 33, 2791), (3, 33, 1235), (3, 44, 4935), (3, 44, 4578), (3, 55, 5786), (3, 55, 2791), (3, 55, 1235);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 1;

SET max_rows_to_read = 4;
SELECT * FROM pk WHERE x = 2 AND y = 11;

SET max_rows_to_read = 5;
SELECT * FROM pk WHERE x = 1;

SET max_rows_to_read = 9;
SELECT * FROM pk WHERE x = 3;

SET max_rows_to_read = 3;
SELECT * FROM pk WHERE x = 3 AND y = 44;

SET max_rows_to_read = 2;
SELECT * FROM pk WHERE x = 3 AND y = 44 AND z = 4935;
SELECT * FROM pk WHERE x = 3 AND y = 44 AND z = 4578;

SET max_rows_to_read = 1;
SELECT * FROM pk WHERE x = 3 AND y = 44 AND z = 4934;
SELECT * FROM pk WHERE x = 3 AND y = 44 AND z = 4936;
SELECT * FROM pk WHERE x = 3 AND y = 44 AND z = 4577;
SELECT * FROM pk WHERE x = 3 AND y = 44 AND z = 4579;

SET max_rows_to_read = 1;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z > 5786;

SET max_rows_to_read = 2;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z >= 5786;

SET max_rows_to_read = 3;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z > 1235;

SET max_rows_to_read = 4;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z >= 1235;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z >= 1000;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z >= 1000 AND x < 10000;
SELECT * FROM pk WHERE x = 3 AND y = 55;
SELECT * FROM pk WHERE x = 3 AND y >= 50;
SELECT * FROM pk WHERE x = 3 AND y > 44;
SELECT * FROM pk WHERE x >= 3 AND y > 44;
SELECT * FROM pk WHERE x > 2 AND y > 44;

SET max_rows_to_read = 2;
SELECT * FROM pk WHERE x = 3 AND y = 55 AND z = 5786;

SET max_rows_to_read = 15;
SET merge_tree_min_rows_for_seek = 0;
SELECT * FROM pk WHERE z = 2791;
SELECT * FROM pk WHERE z = 5786;
SELECT * FROM pk WHERE z = 1235;
SELECT * FROM pk WHERE z = 4578;

SET max_rows_to_read = 10;
SELECT * FROM pk WHERE y = 11;
SELECT * FROM pk WHERE y = 22;
SELECT * FROM pk WHERE y = 33;
SELECT * FROM pk WHERE y = 44;
SELECT * FROM pk WHERE y = 55;

DROP TABLE pk;
