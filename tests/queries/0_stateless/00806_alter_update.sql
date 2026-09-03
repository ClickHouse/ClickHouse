DROP TABLE IF EXISTS alter_update_00806;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE alter_update_00806 (d Date, e Enum8('foo'=1, 'bar'=2)) Engine = MergeTree(d, (d), 8192);
INSERT INTO alter_update_00806 (d, e) VALUES ('2018-01-01', 'foo');
INSERT INTO alter_update_00806 (d, e) VALUES ('2018-01-02', 'bar');

ALTER TABLE alter_update_00806 UPDATE e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d='2018-01-02' SETTINGS mutations_sync = 1;


SELECT e FROM alter_update_00806 ORDER BY d;

DROP TABLE alter_update_00806;
