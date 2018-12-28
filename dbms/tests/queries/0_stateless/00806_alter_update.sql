DROP TABLE IF EXISTS test.alter_update;

CREATE TABLE test.alter_update (d Date, e Enum8('foo'=1, 'bar'=2)) Engine = MergeTree(d, (d), 8192);
INSERT INTO test.alter_update (d, e) VALUES ('2018-01-01', 'foo');
INSERT INTO test.alter_update (d, e) VALUES ('2018-01-02', 'bar');

ALTER TABLE test.alter_update UPDATE e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d='2018-01-02';

SELECT sleep(1); -- TODO: there should be setting for sync ALTER UPDATE someday.

SELECT e FROM test.alter_update ORDER BY d;
