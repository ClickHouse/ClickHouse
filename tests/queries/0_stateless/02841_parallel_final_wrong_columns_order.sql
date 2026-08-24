-- Tags: no-random-merge-tree-settings
-- Because we insert one million rows, it shouldn't choose too low index granularity.

drop table if exists tab2;
create table tab2 (id String, version Int64, l String, accountCode String, z Int32) engine = ReplacingMergeTree(z) PRIMARY KEY (accountCode, id) ORDER BY (accountCode, id, version, l);
insert into tab2 select toString(number), number, toString(number), toString(number), 0 from numbers(1e6);
set max_threads=2;
select count() from tab2 final;
DROP TABLE tab2;
