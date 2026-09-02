DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
CREATE TABLE table1 ( id String ) ENGINE = Log;
CREATE TABLE table2 ( parent_id String ) ENGINE = Log;

insert into table1 values ('1');

SELECT table2.parent_id = '', isNull(table2.parent_id)
FROM table1 ANY LEFT JOIN table2 ON table1.id = table2.parent_id;

SET join_use_nulls = 1;

SELECT table2.parent_id = '', isNull(table2.parent_id)
FROM table1 ANY LEFT JOIN table2 ON table1.id = table2.parent_id;

DROP TABLE table1;
DROP TABLE table2;
