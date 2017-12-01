DROP TABLE IF EXISTS test.summing_merge_tree;

CREATE TABLE test.summing_merge_tree (d Date, a String, x UInt32, y UInt64, z Float64) ENGINE = SummingMergeTree(d, a, 8192);

INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Hello', 1, 2, 3);
INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Hello', 4, 5, 6);
INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Goodbye', 1, 2, 3);

OPTIMIZE TABLE test.summing_merge_tree;
OPTIMIZE TABLE test.summing_merge_tree;
OPTIMIZE TABLE test.summing_merge_tree;

SELECT * FROM test.summing_merge_tree ORDER BY d, a, x, y, z;


DROP TABLE test.summing_merge_tree;

CREATE TABLE test.summing_merge_tree (d Date, a String, x UInt32, y UInt64, z Float64) ENGINE = SummingMergeTree(d, a, 8192, (y, z));

INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Hello', 1, 2, 3);
INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Hello', 4, 5, 6);
INSERT INTO test.summing_merge_tree VALUES ('2000-01-01', 'Goodbye', 1, 2, 3);

OPTIMIZE TABLE test.summing_merge_tree;
OPTIMIZE TABLE test.summing_merge_tree;
OPTIMIZE TABLE test.summing_merge_tree;

SELECT * FROM test.summing_merge_tree ORDER BY d, a, x, y, z;


DROP TABLE test.summing_merge_tree;

--
DROP TABLE IF EXISTS test.summing;
CREATE TABLE test.summing (p Date, k UInt64, s UInt64) ENGINE = SummingMergeTree(p, k, 1);

INSERT INTO test.summing (k, s) VALUES (0, 1);
INSERT INTO test.summing (k, s) VALUES (0, 1), (666, 1), (666, 0);
OPTIMIZE TABLE test.summing PARTITION 197001;

SELECT k, s FROM test.summing ORDER BY k;
