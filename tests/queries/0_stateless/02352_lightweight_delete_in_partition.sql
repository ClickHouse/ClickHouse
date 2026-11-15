DROP TABLE IF EXISTS t_merge_tree SYNC;
DROP TABLE IF EXISTS t_replicated_merge_tree SYNC;

CREATE TABLE t_merge_tree(time Date, id String , name String) ENGINE = MergeTree() PARTITION BY time ORDER BY id;
CREATE TABLE t_replicated_merge_tree(time Date, id String, name String) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_rep','1') PARTITION BY time ORDER BY id;

INSERT INTO t_merge_tree select '2024-08-01', '1', toString(number) FROM numbers(100);
INSERT INTO t_merge_tree select '2024-08-02', '1', toString(number)  FROM numbers(100);

INSERT INTO t_replicated_merge_tree select '2024-08-01', '1', toString(number) FROM numbers(100);
INSERT INTO t_replicated_merge_tree select '2024-08-02', '1', toString(number)  FROM numbers(100);

SELECT COUNT() FROM t_merge_tree;
SELECT COUNT() FROM t_replicated_merge_tree;

DELETE FROM t_merge_tree IN PARTITION '2024-08-01' WHERE id = '1';
DELETE FROM t_replicated_merge_tree IN PARTITION '2024-08-01' WHERE id = '1';

SELECT COUNT() FROM t_merge_tree;
SELECT COUNT() FROM t_replicated_merge_tree;

DROP TABLE t_merge_tree SYNC;
DROP TABLE t_replicated_merge_tree SYNC;
