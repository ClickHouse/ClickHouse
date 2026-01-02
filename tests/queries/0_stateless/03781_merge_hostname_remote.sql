DROP TABLE IF EXISTS merge_host_remote_tab_a;
DROP TABLE IF EXISTS merge_host_remote_tab_b;

CREATE TABLE merge_host_remote_tab_a (number UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO merge_host_remote_tab_a VALUES (1);

CREATE TABLE merge_host_remote_tab_b AS remote('127.0.0.1', numbers(2));

SET enable_analyzer = 1;

SELECT 'before';
SELECT hostName(), * FROM merge(currentDatabase(), '^merge_host_remote_tab_') ORDER BY number FORMAT Null;
SELECT 'after';

DROP TABLE merge_host_remote_tab_a;
DROP TABLE merge_host_remote_tab_b;
