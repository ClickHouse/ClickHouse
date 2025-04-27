SET log_queries = 1;

CREATE OR REPLACE TABLE users (uid Int16, name String, age Int16, parent_id Int16) 
ENGINE = MergeTree()
ORDER BY uid;
ALTER TABLE users ADD INDEX age_i age TYPE set(10) GRANULARITY 2;
ALTER TABLE users ADD INDEX name_i name TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1;

INSERT INTO users VALUES (1231, 'John', 33, 1);
INSERT INTO users VALUES (6666, 'Ksenia', 48, 2);
INSERT INTO users VALUES (8888, 'Alice', 50, 2);

CREATE OR REPLACE TABLE parents (uid Int16, name String, age Int16) 
ENGINE = MergeTree()
ORDER BY uid;
ALTER TABLE parents ADD INDEX age_i age TYPE set(10) GRANULARITY 2;

INSERT INTO parents VALUES (1, 'Carry', 100);
INSERT INTO parents VALUES (2, 'Jim', 99);
INSERT INTO parents VALUES (2, 'Mandy', 80);
ALTER TABLE parents ADD INDEX name_i name TYPE tokenbf_v1(8192, 1, 0) GRANULARITY 1;




SELECT * FROM users FORMAT Null SETTINGS log_comment='1';
SELECT * FROM users WHERE age = 33 FORMAT Null SETTINGS log_comment='2';
SELECT * FROM users WHERE startsWith(name, 'Al') FORMAT Null SETTINGS log_comment='3';
SELECT * FROM users WHERE age = 33 AND name = 'Alice' FORMAT Null SETTINGS log_comment='4';
SELECT * FROM users 
LEFT JOIN parents ON users.parent_id = parents.uid 
WHERE parents.age >= 90 AND users.name = 'Alice' 
FORMAT Null SETTINGS log_comment='5';

SYSTEM FLUSH LOGS system.query_log;

SELECT arraySort(skip_indexes)
FROM system.query_log
WHERE
    event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment in ('1', '2', '3', '4', '5')
ORDER BY log_comment asc, event_time desc 
limit 1 by log_comment;
