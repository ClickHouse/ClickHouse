-- Tags: no-fasttest

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = '';
SET query_plan_enable_optimizations = 0;
ALTER TABLE t0 MODIFY SETTING auto_statistics_types = 'uniq, countmin';
INSERT INTO TABLE t0 (c0) VALUES (1);
ALTER TABLE t0 MODIFY SETTING auto_statistics_types = 'countmin';
ALTER TABLE t0 APPLY DELETED MASK IN PARTITION ID '1';
