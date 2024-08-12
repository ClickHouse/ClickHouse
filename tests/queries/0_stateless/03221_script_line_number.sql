



SET log_comment = 'script_line_number_test', log_queries = 1;

DROP DATABASE IF EXISTS 
03221_db;
DROP DATABASE IF EXISTS 
03221_db1;


DROP DATABASE IF EXISTS 
03221_db2;

CREATE DATABASE 03221_db;
CREATE DATABASE
03221_db1;
CREATE DATABASE

03221_db2;


CREATE TABLE 03221_db.t   (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE 03221_db1.t1 (n Int8) ENGINE=MergeTree ORDER BY n;
CREATE TABLE 03221_db2.t2 (n Int8) ENGINE=MergeTree ORDER BY n;

INSERT INTO 03221_db.t   
SELECT * FROM numbers(10);

INSERT 
INTO 
03221_db1.t1
SELECT
*
FROM
numbers(10);

INSERT INTO 03221_db2.t2 

SELECT * FROM numbers(10);




DROP 
DATABASE 03221_db;
DROP DATABASE
03221_db1;
DROP
DATABASE
03221_db2;

SYSTEM FLUSH LOGS;
SELECT type, log_comment, query, script_line_number FROM system.query_log WHERE log_comment = 'script_line_number_test' AND type = 1 AND current_database = currentDatabase();