DROP TABLE IF EXISTS partition_id;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE IF NOT EXISTS partition_id (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO partition_id VALUES (100, 1), (200, 2), (300, 3);

SELECT _partition_id FROM partition_id ORDER BY x;

DROP TABLE IF EXISTS partition_id;

