drop table if exists t1;
SET allow_experimental_statistics = 1;
create table t1 (d Datetime, c0 Int TTL d + INTERVAL 1 MONTH SETTINGS (max_compress_block_size = 1), c2 Int STATISTICS(Uniq) SETTINGS (max_compress_block_size = 1)) Engine = MergeTree() ORDER BY ();
insert into t1 values ('2024-11-15 18:30:00', 25, 20);

