-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas

--- #65607
select 'index is applied while using column alias';

drop table if exists t;

CREATE TABLE t
(
    `tenant` String,
    `recordTimestamp` Int64,
    `responseBody` String,
    `colAlias` String ALIAS responseBody || 'something else',
    INDEX ngrams colAlias TYPE ngrambf_v1(3, 2097152, 3, 0) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY recordTimestamp
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t SELECT toString(number), number, toString(number) from numbers(65536);

explain indexes=1 select tenant,recordTimestamp from t where colAlias like '%abcd%' settings enable_analyzer=0;
explain indexes=1 select tenant,recordTimestamp from t where colAlias like '%abcd%' settings enable_analyzer=1;


--- #69373
select 'index is applied to view';

drop table if exists tab_v1;
CREATE TABLE tab_v1
(
    content String,
    INDEX idx_content_bloom content TYPE bloom_filter(0.01)
)
ENGINE = MergeTree
ORDER BY content;

drop table if exists tab_v3;
CREATE VIEW tab_v3
AS SELECT * FROM tab_v1;

INSERT INTO tab_v1 (content) VALUES ('aaa bbb'), ('ccc ddd');

SELECT count()
FROM tab_v3
WHERE content = 'iii'
SETTINGS force_data_skipping_indices='idx_content_bloom', enable_analyzer=0;

SELECT count()
FROM tab_v3
WHERE content = 'iii'
SETTINGS force_data_skipping_indices='idx_content_bloom', enable_analyzer=1;
