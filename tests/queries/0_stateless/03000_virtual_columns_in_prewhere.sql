SET optimize_trivial_insert_select = 1;

drop table if exists x;

create table x (i int, j int, k int) engine MergeTree order by tuple() settings index_granularity=8192, index_granularity_bytes = '10Mi',  min_bytes_for_wide_part=0, min_rows_for_wide_part=0, ratio_of_defaults_for_sparse_serialization=1;

insert into x select number, number * 2, number * 3 from numbers(100000);

-- One granule, (_part_offset (8 bytes) + <one minimal physical column> (4 bytes)) * 8192 + <other two physical columns>(8 bytes) * 1 = 98312
select * from x prewhere _part_offset = 0 settings max_bytes_to_read = 98312;

drop table x;
