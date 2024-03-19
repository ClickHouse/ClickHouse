DROP TABLE IF EXISTS test;
CREATE TABLE test (x String, y Int32) ENGINE = MergeTree() ORDER BY x;

INSERT INTO test VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5);

select * from test where x in ('a') SETTINGS allow_experimental_analyzer = 1;
select '-------------------';
explain query tree select * from test where x in ('a') SETTINGS allow_experimental_analyzer = 1;
select '-------------------';
explain query tree select * from test where x in ('a','b') SETTINGS allow_experimental_analyzer = 1;