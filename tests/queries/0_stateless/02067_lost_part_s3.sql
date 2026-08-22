-- Tags: no-fasttest

DROP TABLE IF EXISTS partslost_0;
DROP TABLE IF EXISTS partslost_1;
DROP TABLE IF EXISTS partslost_2;

CREATE TABLE partslost_0 (x String) ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}_02067_lost/partslost', '0') ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, old_parts_lifetime = 1,
    cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration=0,
    index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE partslost_1 (x String) ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}_02067_lost/partslost', '1') ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, old_parts_lifetime = 1,
    cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration=0,
    index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE partslost_2 (x String) ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}_02067_lost/partslost', '2') ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, old_parts_lifetime = 1,
    cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, cleanup_thread_preferred_points_per_iteration=0,
    index_granularity = 8192, index_granularity_bytes = '10Mi';


INSERT INTO partslost_0 SELECT toString(number) AS x from system.numbers LIMIT 10000;

ALTER TABLE partslost_0 ADD INDEX idx x TYPE tokenbf_v1(285000, 3, 12345) GRANULARITY 3;

SET mutations_sync = 2;

ALTER TABLE partslost_0 MATERIALIZE INDEX idx;

-- In worst case doesn't check anything, but it's not flaky
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;

ALTER TABLE partslost_0 DROP INDEX idx;

select count() from partslost_0;
select count() from partslost_1;
select count() from partslost_2;

DROP TABLE IF EXISTS partslost_0;
DROP TABLE IF EXISTS partslost_1;
DROP TABLE IF EXISTS partslost_2;
