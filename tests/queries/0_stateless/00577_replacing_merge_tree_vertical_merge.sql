set optimize_on_insert = 0;

drop table if exists tab_00577;
create table tab_00577 (date Date, version UInt64, val UInt64) engine = ReplacingMergeTree(version) partition by date order by date settings enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0, min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0, allow_experimental_replacing_merge_with_cleanup=1;
insert into tab_00577 values ('2018-01-01', 2, 2), ('2018-01-01', 1, 1);
insert into tab_00577 values ('2018-01-01', 0, 0);
select * from tab_00577 order by version;
OPTIMIZE TABLE tab_00577 FINAL CLEANUP;
select * from tab_00577;
drop table tab_00577;


DROP TABLE IF EXISTS testCleanupR1;
CREATE TABLE testCleanupR1 (uid String, version UInt32, is_deleted UInt8)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/test_cleanup/', 'r1', version, is_deleted)
    ORDER BY uid SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0, min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0, allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO testCleanupR1 (*) VALUES ('d1', 1, 0),('d2', 1, 0),('d3', 1, 0),('d4', 1, 0);
INSERT INTO testCleanupR1 (*) VALUES ('d3', 2, 1);
INSERT INTO testCleanupR1 (*) VALUES ('d1', 2, 1);
SYSTEM SYNC REPLICA testCleanupR1; -- Avoid "Cannot select parts for optimization: Entry for part all_2_2_0 hasn't been read from the replication log yet"

OPTIMIZE TABLE testCleanupR1 FINAL CLEANUP;

-- Only d3 to d5 remain
SELECT '== (Replicas) Test optimize ==';
SELECT * FROM testCleanupR1 order by uid;
DROP TABLE IF EXISTS testCleanupR1