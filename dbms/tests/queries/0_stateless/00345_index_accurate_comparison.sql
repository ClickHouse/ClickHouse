DROP TABLE IF EXISTS test.index;

CREATE TABLE test.index
(
    key Int32,
    name String,
    merge_date Date
) ENGINE = MergeTree(merge_date, key, 8192);

insert into test.index values (1,'1','2016-07-07');
insert into test.index values (-1,'-1','2016-07-07');

select * from test.index where key = 1;
select * from test.index where key = -1;
OPTIMIZE TABLE test.index;
select * from test.index where key = 1;
select * from test.index where key = -1;
select * from test.index where key < -0.5;

DROP TABLE test.index;
