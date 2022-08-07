DROP TABLE IF EXISTS view_foo_bar;
DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS bar;

set allow_deprecated_syntax_for_merge_tree=1;
create table foo (ddate Date, id Int64, n String) ENGINE = ReplacingMergeTree(ddate, (id), 8192);
create table bar (ddate Date, id Int64, n String, foo_id Int64) ENGINE = ReplacingMergeTree(ddate, (id), 8192);
insert into bar (id, n, foo_id) values (1, 'bar_n_1', 1);
create MATERIALIZED view view_foo_bar ENGINE = ReplacingMergeTree(ddate, (bar_id), 8192) as select ddate, bar_id, bar_n, foo_id, foo_n from (select ddate, id as bar_id, n as bar_n, foo_id from bar) js1 any left join (select id as foo_id, n as foo_n from foo) js2 using foo_id;
insert into bar (id, n, foo_id) values (1, 'bar_n_1', 1);
SELECT * FROM view_foo_bar;

DROP TABLE view_foo_bar;
DROP TABLE foo;
DROP TABLE bar;
