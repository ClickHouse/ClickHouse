-- Test for FINAL query on ReplacingMergeTree + is_deleted makes use of optimizations.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
     pkey String,
     id Int32,
     v Int32,
     version UInt64,
     is_deleted UInt8
) Engine = ReplacingMergeTree(version,is_deleted)
PARTITION BY pkey ORDER BY id
SETTINGS index_granularity=512;

-- insert 10000 rows in partition 'A' and delete half of them and merge the 2 parts
INSERT INTO tab SELECT 'A', number, number, 1, 0 FROM numbers(10000);
INSERT INTO tab SELECT 'A', number, number + 1, 2, IF(number % 2 = 0, 0, 1) FROM numbers(10000);

OPTIMIZE TABLE tab SETTINGS mutations_sync = 2;

SYSTEM STOP MERGES tab;

-- insert 10000 rows in partition 'B' and delete half of them, but keep 2 parts
INSERT INTO tab SELECT 'B', number+1000000, number, 1, 0 FROM numbers(10000);
INSERT INTO tab SELECT 'B', number+1000000, number + 1, 2, IF(number % 2 = 0, 0, 1) FROM numbers(10000);

SET do_not_merge_across_partitions_select_final=1;

-- verify : 10000 rows expected
SELECT count()
FROM tab FINAL;

-- add a filter : 9950 rows expected
SELECT count()
FROM tab FINAL
WHERE id >= 100;

-- only even id's are left - 0 rows expected
SELECT count()
FROM tab FINAL
WHERE (id % 2) = 1; 

-- 10000 rows expected
SELECT count()
FROM tab FINAL
WHERE (id % 2) = 0; 

-- create some more partitions
INSERT INTO tab SELECT 'C', number+2000000, number, 1, 0 FROM numbers(100);

-- insert and delete some rows to get intersecting/non-intersecting ranges in same partition
INSERT INTO tab SELECT 'D', number+3000000, number, 1, 0 FROM numbers(10000);
INSERT INTO tab SELECT 'D', number+3000000, number + 1, 1, IF(number % 2 = 0, 0, 1) FROM numbers(5000);

INSERT INTO tab SELECT 'E', number+4000000, number, 1, 0 FROM numbers(100);

-- Total 10000 (From A & B) + 100 (From C) + 7500 (From D) + 100 (From E) = 17700 rows
SELECT count()
FROM tab FINAL
SETTINGS do_not_merge_across_partitions_select_final=0,split_intersecting_parts_ranges_into_layers_final=0;

SELECT count()
FROM tab FINAL
SETTINGS do_not_merge_across_partitions_select_final=1,split_intersecting_parts_ranges_into_layers_final=1;

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL SETTINGS mutations_sync = 2;

SELECT count()
FROM tab FINAL;

DROP TABLE IF EXISTS tab;
