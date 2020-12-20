DROP TABLE IF EXISTS testtbl;

CREATE TABLE testtbl (`id` String, `dt` String)
ENGINE = MergeTree() PARTITION BY dt ORDER BY (id);

INSERT INTO testtbl VALUES('1', '2020-01-01');

SELECT dt, count(distinct id) as dst_cnt
FROM remote('localhost,127.0.0.1',currentDatabase(),'testtbl')
GROUP BY dt;

SELECT '---';

SELECT dt, count(distinct id) as dst_cnt
FROM remote('localhost,127.0.0.1',currentDatabase(),'testtbl')
GROUP BY dt SETTINGS distributed_group_by_no_merge=1;

SELECT '---';

SELECT dt, count(distinct id) as dst_cnt
FROM remote('localhost,127.0.0.1',currentDatabase(),'testtbl')
GROUP BY dt SETTINGS distributed_group_by_merge_finalized=true;

DROP TABLE testtbl;
