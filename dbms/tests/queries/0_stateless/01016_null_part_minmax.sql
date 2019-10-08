-- this test checks that null values are correctly serialized inside minmax index (issue #7113)
create table null_01016 (x Nullable(String)) engine MergeTree order by ifNull(x, 'order-null') partition by ifNull(x, 'partition-null');
insert into null_01016 values (null);
