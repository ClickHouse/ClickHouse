DROP TABLE IF EXISTS test.drop_column;
CREATE TABLE test.drop_column (d Date, num Int64, str String) ENGINE = MergeTree(d, d, 8192);

insert into test.drop_column values ('2016-12-12', 1, 'a'), ('2016-11-12', 2, 'b');

SELECT num, str FROM test.drop_column ORDER BY num;
alter table test.drop_column drop column num from partition '201612';
SELECT num, str FROM test.drop_column ORDER BY num;

DROP TABLE test.drop_column;
