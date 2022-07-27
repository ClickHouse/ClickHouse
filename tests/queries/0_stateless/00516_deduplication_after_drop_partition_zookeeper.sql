-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP TABLE IF EXISTS deduplication_by_partition;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE deduplication_by_partition(d Date, x UInt32) ENGINE =
    ReplicatedMergeTree('/clickhouse/tables/{database}/test_00516/deduplication_by_partition', 'r1', d, x, 8192);

INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
INSERT INTO deduplication_by_partition VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);

SELECT '*** Before DROP PARTITION ***';

SELECT * FROM deduplication_by_partition ORDER BY d, x;

ALTER TABLE deduplication_by_partition DROP PARTITION 200001;

SELECT '*** After DROP PARTITION ***';

SELECT * FROM deduplication_by_partition ORDER BY d, x;

INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition VALUES ('2000-01-01', 4);
INSERT INTO deduplication_by_partition VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
INSERT INTO deduplication_by_partition VALUES ('2000-02-01', 6), ('2000-02-01', 7);

SELECT '*** After INSERT ***';

SELECT * FROM deduplication_by_partition ORDER BY d, x;

DROP TABLE deduplication_by_partition;
