CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.pk_order;

SET optimize_pk_order = 1;

CREATE TABLE test.pk_order(a UInt64, b UInt64, c UInt64, d UInt64) ENGINE=MergeTree() ORDER BY (a, b);
INSERT INTO test.pk_order(a, b, c, d) VALUES (1, 1, 101, 1), (1, 2, 102, 1), (1, 3, 103, 1), (1, 4, 104, 1);;
INSERT INTO test.pk_order(a, b, c, d)  VALUES (1, 5, 104, 1), (1, 6, 105, 1), (2, 1, 106, 2), (2, 1, 107, 2);

INSERT INTO test.pk_order(a, b, c, d) VALUES (2, 2, 107, 2), (2, 3, 108, 2), (2, 4, 109, 2);

SELECT b FROM test.pk_order ORDER BY a, b;
SELECT a FROM test.pk_order ORDER BY a, b;
SELECT c FROM test.pk_order ORDER BY a, b;
SELECT d FROM test.pk_order ORDER BY a, b;
SELECT d FROM test.pk_order ORDER BY a;


SELECT b FROM test.pk_order ORDER BY a, b DESC;
SELECT a FROM test.pk_order ORDER BY a, b DESC;
SELECT c FROM test.pk_order ORDER BY a, b DESC;
SELECT d FROM test.pk_order ORDER BY a, b DESC;
SELECT d FROM test.pk_order ORDER BY a DESC;

DROP TABLE IF EXISTS test.pk_order;


drop table if exists tab_00717;
create table tab_00717 (a String, b StringWithDictionary) engine = MergeTree order by a;
insert into tab_00717 values ('a_1', 'b_1'), ('a_2', 'b_2');
select a from tab_00717 group by a order by a;
drop table if exists tab_00717;
