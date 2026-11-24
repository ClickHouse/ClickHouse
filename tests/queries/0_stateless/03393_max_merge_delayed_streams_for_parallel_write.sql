-- Tags: no-fasttest
-- - no-fasttest -- S3 is required

-- Vertical merge of a wide table on S3: every column stream allocates ~1 MiB buffers, which are kept
-- until the stream is finished. `max_merge_delayed_streams_for_parallel_write` bounds the number of
-- such streams, so the peak memory usage of the merge is roughly proportional to it.
-- With 200 columns: ~25 MiB with the limit of 10, and ~200+ MiB without the limit.

create table t_wide (c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64, c16 UInt64, c17 UInt64, c18 UInt64, c19 UInt64, c20 UInt64, c21 UInt64, c22 UInt64, c23 UInt64, c24 UInt64, c25 UInt64, c26 UInt64, c27 UInt64, c28 UInt64, c29 UInt64, c30 UInt64, c31 UInt64, c32 UInt64, c33 UInt64, c34 UInt64, c35 UInt64, c36 UInt64, c37 UInt64, c38 UInt64, c39 UInt64, c40 UInt64, c41 UInt64, c42 UInt64, c43 UInt64, c44 UInt64, c45 UInt64, c46 UInt64, c47 UInt64, c48 UInt64, c49 UInt64, c50 UInt64, c51 UInt64, c52 UInt64, c53 UInt64, c54 UInt64, c55 UInt64, c56 UInt64, c57 UInt64, c58 UInt64, c59 UInt64, c60 UInt64, c61 UInt64, c62 UInt64, c63 UInt64, c64 UInt64, c65 UInt64, c66 UInt64, c67 UInt64, c68 UInt64, c69 UInt64, c70 UInt64, c71 UInt64, c72 UInt64, c73 UInt64, c74 UInt64, c75 UInt64, c76 UInt64, c77 UInt64, c78 UInt64, c79 UInt64, c80 UInt64, c81 UInt64, c82 UInt64, c83 UInt64, c84 UInt64, c85 UInt64, c86 UInt64, c87 UInt64, c88 UInt64, c89 UInt64, c90 UInt64, c91 UInt64, c92 UInt64, c93 UInt64, c94 UInt64, c95 UInt64, c96 UInt64, c97 UInt64, c98 UInt64, c99 UInt64, c100 UInt64, c101 UInt64, c102 UInt64, c103 UInt64, c104 UInt64, c105 UInt64, c106 UInt64, c107 UInt64, c108 UInt64, c109 UInt64, c110 UInt64, c111 UInt64, c112 UInt64, c113 UInt64, c114 UInt64, c115 UInt64, c116 UInt64, c117 UInt64, c118 UInt64, c119 UInt64, c120 UInt64, c121 UInt64, c122 UInt64, c123 UInt64, c124 UInt64, c125 UInt64, c126 UInt64, c127 UInt64, c128 UInt64, c129 UInt64, c130 UInt64, c131 UInt64, c132 UInt64, c133 UInt64, c134 UInt64, c135 UInt64, c136 UInt64, c137 UInt64, c138 UInt64, c139 UInt64, c140 UInt64, c141 UInt64, c142 UInt64, c143 UInt64, c144 UInt64, c145 UInt64, c146 UInt64, c147 UInt64, c148 UInt64, c149 UInt64, c150 UInt64, c151 UInt64, c152 UInt64, c153 UInt64, c154 UInt64, c155 UInt64, c156 UInt64, c157 UInt64, c158 UInt64, c159 UInt64, c160 UInt64, c161 UInt64, c162 UInt64, c163 UInt64, c164 UInt64, c165 UInt64, c166 UInt64, c167 UInt64, c168 UInt64, c169 UInt64, c170 UInt64, c171 UInt64, c172 UInt64, c173 UInt64, c174 UInt64, c175 UInt64, c176 UInt64, c177 UInt64, c178 UInt64, c179 UInt64, c180 UInt64, c181 UInt64, c182 UInt64, c183 UInt64, c184 UInt64, c185 UInt64, c186 UInt64, c187 UInt64, c188 UInt64, c189 UInt64, c190 UInt64, c191 UInt64, c192 UInt64, c193 UInt64, c194 UInt64, c195 UInt64, c196 UInt64, c197 UInt64, c198 UInt64, c199 UInt64)
engine = MergeTree
partition by ()
order by ()
settings
    -- cache has its own problems (see filesystem_cache_prefer_bigger_buffer_size)
    storage_policy = 's3_no_cache',
    -- horizontal merges open all streams at once, so they will use a huge amount of memory regardless of the setting
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_full_part_storage = 0,
    -- the adaptive write buffer would hide the per-stream allocation that the setting bounds
    min_columns_to_activate_adaptive_write_buffer = 0,
    max_merge_delayed_streams_for_parallel_write = 10,
    -- avoid superfluous merges
    merge_selector_base = 1000,
    auto_statistics_types = '';

insert into t_wide select * from generateRandom() limit 10 settings optimize_trivial_insert_select = 0;

optimize table t_wide final;
system flush logs part_log;
select 'max_merge_delayed_streams_for_parallel_write=10' as test, * from system.part_log where table = 't_wide' and database = currentDatabase() and event_date >= yesterday() and event_time >= now() - 600 and event_type = 'MergeParts' and peak_memory_usage > 100_000_000 format Vertical;

-- Control: without the limit, the same merge must exceed the threshold, otherwise the check above is meaningless.
alter table t_wide modify setting max_merge_delayed_streams_for_parallel_write = 10000;

optimize table t_wide final;
system flush logs part_log;
select 'max_merge_delayed_streams_for_parallel_write=10000' as test, count() as count from system.part_log where table = 't_wide' and database = currentDatabase() and event_date >= yesterday() and event_time >= now() - 600 and event_type = 'MergeParts' and peak_memory_usage > 100_000_000;
