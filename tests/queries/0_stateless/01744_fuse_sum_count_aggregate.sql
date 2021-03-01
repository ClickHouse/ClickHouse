DROP TABLE IF EXISTS fuse_tbl;
CREATE TABLE fuse_tbl(a Int8, b Int8) Engine = Log;
INSERT INTO fuse_tbl SELECT number, number + 1 FROM numbers(1, 20);

set optimize_fuse_sum_count_avg = 1;
SELECT sum(a), sum(b), count(b) from fuse_tbl;
EXPLAIN SYNTAX SELECT sum(a), sum(b), count(b) from fuse_tbl;
DROP TABLE fuse_tbl;
