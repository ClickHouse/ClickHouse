-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: always returns rows_before_limit_counter in response

drop table if exists 03644_data;

create table 03644_data (i UInt32) engine = MergeTree order by i
as
select number from numbers(10000);

select i
from 03644_data
group by i
having count() > 1
settings
    rows_before_aggregation = 1,
    exact_rows_before_limit = 1,
    output_format_write_statistics = 0,
    max_block_size = 100,
    aggregation_in_order_max_block_bytes = 8,
    optimize_aggregation_in_order=1
format JSONCompact;

drop table 03644_data;
