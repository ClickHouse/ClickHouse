-- Check that skip indexes are logged correctly in scenarios like filtering by single 
-- and multiple indexed columns, and in JOINs with indexed join key in right table.
-- Tags: no-parallel-replicas
SET log_queries = 1;
DROP TABLE IF EXISTS children;
CREATE TABLE children (
    `uid` Int16,
    `name` String,
    age Int16,
    parent_id Int16,
    INDEX age_i age TYPE minmax GRANULARITY 1,
    INDEX name_i `name` TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1
    )
ENGINE = MergeTree()
ORDER BY `uid`
SETTINGS index_granularity = 1;

INSERT INTO children VALUES (1, 'Bob', 3, 1);
INSERT INTO children VALUES (2, 'Grant', 4, 2);
INSERT INTO children VALUES (3, 'Alice', 5, 2);

DROP TABLE IF EXISTS parents;
CREATE TABLE parents (
    `uid` Int16,
    `name` String,
    age Int16,
    INDEX age_i age TYPE set(10) GRANULARITY 1
    )
ENGINE = MergeTree()
ORDER BY `uid`
SETTINGS index_granularity = 1;

INSERT INTO parents VALUES (1, 'Leslie', 24);
INSERT INTO parents VALUES (2, 'Tom', 25);
INSERT INTO parents VALUES (3, 'Jack', 26);
ALTER TABLE parents ADD INDEX name_i name TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1;

SELECT * FROM children FORMAT Null SETTINGS log_comment='1', use_skip_indexes_on_data_read=1; -- no skip indexes used
SELECT * FROM children WHERE age = 3 FORMAT Null SETTINGS log_comment='2', use_skip_indexes_on_data_read=1; -- age_i used
SELECT * FROM children WHERE hasToken(name, 'Alice') FORMAT Null SETTINGS log_comment='3', use_skip_indexes_on_data_read=1; -- name_i used
SELECT * FROM children WHERE age = 3 AND name = 'Alice' FORMAT Null SETTINGS log_comment='4', use_skip_indexes_on_data_read=1; -- age_i and name_i used
SELECT * FROM children
LEFT JOIN parents ON children.parent_id = parents.uid
WHERE parents.age >= 25 AND children.name = 'Alice'
FORMAT Null SETTINGS log_comment='5', use_skip_indexes_on_data_read=1; -- age_i and name_i used

-- Test with identifiers that require escaping: table "hello, world" and index "my.favorite.index"
DROP TABLE IF EXISTS `hello, world`;
CREATE TABLE `hello, world` (
    `uid` Int16,
    age Int16,
    INDEX `my.favorite.index` age TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY `uid`
SETTINGS index_granularity = 1;

INSERT INTO `hello, world` VALUES (1, 30);
INSERT INTO `hello, world` VALUES (2, 5); -- age=5 does not match WHERE age=30, causing the index to drop this granule

SELECT * FROM `hello, world` WHERE age = 30 FORMAT Null SETTINGS log_comment='6', use_skip_indexes_on_data_read=1; -- my.favorite.index used

DROP TABLE `hello, world`;

SYSTEM FLUSH LOGS system.query_log;

SELECT formatQuerySingleLine(query), skip_indices
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment in ('1', '2', '3', '4', '5', '6')
ORDER BY log_comment ASC, event_time DESC
LIMIT 1 BY log_comment;
