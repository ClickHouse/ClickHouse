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
    INDEX age_i age TYPE set(10) GRANULARITY 2,
    INDEX name_i `name` TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1
    ) 
ENGINE = MergeTree()
ORDER BY `uid`;

INSERT INTO children VALUES (1, 'Bob', 3, 1);
INSERT INTO children VALUES (2, 'Grant', 4, 2);
INSERT INTO children VALUES (3, 'Alice', 5, 2);

DROP TABLE IF EXISTS parents;
CREATE TABLE parents (
    `uid` Int16, 
    `name` String, 
    age Int16,
    INDEX age_i age TYPE set(10) GRANULARITY 2
    ) 
ENGINE = MergeTree()
ORDER BY `uid`;

INSERT INTO parents VALUES (1, 'Leslie', 24);
INSERT INTO parents VALUES (2, 'Tom', 25);
INSERT INTO parents VALUES (3, 'Jack', 26);
ALTER TABLE parents ADD INDEX name_i name TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1;

SELECT * FROM children FORMAT Null SETTINGS log_comment='1'; -- no skip indexes used
SELECT * FROM children WHERE age = 3 FORMAT Null SETTINGS log_comment='2'; -- age_i used
SELECT * FROM children WHERE startsWith(name, 'Al') FORMAT Null SETTINGS log_comment='3'; -- name_i used
SELECT * FROM children WHERE age = 3 AND name = 'Alice' FORMAT Null SETTINGS log_comment='4'; -- age_i and name_i used
SELECT * FROM children 
LEFT JOIN parents ON children.parent_id = parents.uid 
WHERE parents.age >= 25 AND children.name = 'Alice' 
FORMAT Null SETTINGS log_comment='5'; -- age_i and name_i used

SYSTEM FLUSH LOGS system.query_log;

SELECT formatQuerySingleLine(query), skip_indices
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment in ('1', '2', '3', '4', '5')
ORDER BY log_comment ASC, event_time DESC 
LIMIT 1 BY log_comment;

DROP TABLE IF EXISTS merged_indices;
CREATE TABLE merged_indices (
    `uid` Int16,
    first_num_val Int16,
    second_num_val Int16,
    third_num_val Int16,
    extra String,
    INDEX fnv (first_num_val < second_num_val) TYPE hypothesis GRANULARITY 1,
    INDEX snv (second_num_val < third_num_val) TYPE hypothesis GRANULARITY 1,
    INDEX extra_i extra TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1
    )
ENGINE = MergeTree()
ORDER BY `uid`
SETTINGS index_granularity = 1;

INSERT INTO merged_indices VALUES (1, 1, 2, 3, 'Spots'), (2, 2, 1, 0, 'Dots'), (3, 5, 5, 10, 'Elipses');

SELECT count() FROM merged_indices FORMAT Null SETTINGS log_comment='6'; -- no skip index used
SELECT count() FROM merged_indices WHERE first_num_val > second_num_val FORMAT Null SETTINGS log_comment='7'; -- fnv, snv indices merged
SELECT count() FROM merged_indices WHERE second_num_val < 10 and extra like '%s%' FORMAT Null SETTINGS log_comment='8'; -- all three indices merged

SYSTEM FLUSH LOGS system.query_log;

SELECT formatQuerySingleLine(query), skip_indices
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment in ('6', '7', '8')
ORDER BY log_comment ASC, event_time DESC
LIMIT 1 BY log_comment;
