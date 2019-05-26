DROP TABLE IF EXISTS alter_update;

CREATE TABLE alter_update (d Date, e Enum8('foo'=1, 'bar'=2)) Engine = MergeTree(d, (d), 8192);
INSERT INTO alter_update (d, e) VALUES ('2018-01-01', 'foo');
INSERT INTO alter_update (d, e) VALUES ('2018-01-02', 'bar');

ALTER TABLE alter_update UPDATE e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d='2018-01-02';

SELECT sleep(1); -- TODO: there should be setting for sync ALTER UPDATE someday.

SELECT e FROM alter_update ORDER BY d;
