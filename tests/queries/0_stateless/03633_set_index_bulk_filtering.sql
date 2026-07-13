DROP TABLE IF EXISTS tbulk;
CREATE TABLE tbulk (
    g UInt64, s Int32, k Int64, x UInt64,
    INDEX gset g TYPE set (0) GRANULARITY 100
)
engine=MergeTree
order by (x, k, s)
as select number%3, 1, 4, number%10 from numbers(1e6);

select count(x) from tbulk where g = 1 and k = 1 settings secondary_indices_enable_bulk_filtering=0;
select count(x) from tbulk where g = 1 and k = 1 settings secondary_indices_enable_bulk_filtering=1;