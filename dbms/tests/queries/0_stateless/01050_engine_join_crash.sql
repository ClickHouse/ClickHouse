DROP TABLE IF EXISTS testJoinTable;

SET any_join_distinct_right_table_keys = 1;
SET enable_optimize_predicate_expression = 0;

CREATE TABLE testJoinTable (number UInt64, data String) ENGINE = Join(ANY, INNER, number) SETTINGS any_join_distinct_right_table_keys = 1;

INSERT INTO testJoinTable VALUES (1, '1'), (2, '2'), (3, '3');

SELECT * FROM (SELECT * FROM numbers(10)) js1 INNER JOIN testJoinTable USING number; -- { serverError 264 }
SELECT * FROM (SELECT * FROM numbers(10)) js1 INNER JOIN (SELECT * FROM testJoinTable) js2 USING number;
SELECT * FROM (SELECT * FROM numbers(10)) js1 ANY INNER JOIN testJoinTable USING number;
SELECT * FROM testJoinTable;

DROP TABLE testJoinTable;

SELECT '-';
 
DROP TABLE IF EXISTS master;
DROP TABLE IF EXISTS transaction;

CREATE TABLE transaction (id Int32, value Float64, master_id Int32) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE master (id Int32, name String) ENGINE = Join (ANY, LEFT, id) SETTINGS any_join_distinct_right_table_keys = 1;

INSERT INTO master VALUES (1, 'ONE');
INSERT INTO transaction VALUES (1, 52.5, 1);

SELECT tx.id, tx.value, m.name FROM transaction tx ANY LEFT JOIN master m ON m.id = tx.master_id;

DROP TABLE master;
DROP TABLE transaction;

SELECT '-';

DROP TABLE IF EXISTS some_join;
DROP TABLE IF EXISTS tbl;

CREATE TABLE tbl (eventDate Date, id String) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY eventDate;
CREATE TABLE some_join (id String, value String) ENGINE = Join(ANY, LEFT, id) SETTINGS any_join_distinct_right_table_keys = 1;

SELECT * FROM tbl AS t ANY LEFT JOIN some_join USING (id);
SELECT * FROM tbl AS t ANY LEFT JOIN some_join AS d USING (id);
-- TODO SELECT t.*, d.* FROM tbl AS t ANY LEFT JOIN some_join AS d USING (id);

DROP TABLE some_join;
DROP TABLE tbl;
