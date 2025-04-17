CREATE TABLE tdata0 (k Int64, dt DateTime, s String)
ENGINE=ReplacingMergeTree
order by (k,dt);

create table tmerge as tdata0 Engine=Merge('default', 'tdata');

INSERT INTO tdata0
select number, '2020-01-01 00:00:00', '' from numbers(10);

select count() from tmerge final prewhere dt >= '2020-01-01 00:00:00';
