-- Regression test: the compact `EXPLAIN indexes=1` fast path (which collapses
-- supported plan shapes such as `Merge` tables and homogeneous `UNION ALL`
-- into a single rolled-up index summary) must not be taken by
-- `EXPLAIN ANALYZE`, whose defaults are `compact=1, indexes=1`. Otherwise the
-- per-step runtime stats (`I/O:` / `time` lines) would be lost.
-- Since the stats values are non-deterministic, we assert structural
-- invariants via string matching on the `explain` column.

set enable_analyzer = 1;

drop table if exists test_ea_compact_mt;
drop table if exists test_ea_compact_merge;

create table test_ea_compact_mt (key Int, value Int, index value_idx value type minmax granularity 1) engine=MergeTree() order by key settings index_granularity=1000;
insert into test_ea_compact_mt select number, number*100 from numbers(10000) settings max_block_size=100000, min_insert_block_size_rows=100000, max_insert_threads=1;

create table test_ea_compact_merge (key Int, value Int) engine=Merge(currentDatabase(), '^test_ea_compact_mt$');

-- Homogeneous `UNION ALL`: per-step stats must be printed for both branches.
select
    countIf(explain like '%I/O: rows%') >= 2,
    countIf(explain like '%parallelism%') >= 2,
    countIf(explain like '%Query summary:%') = 1
from (explain analyze select count() from (select key from test_ea_compact_mt where value = 100 union all select key from test_ea_compact_mt where value = 200));

-- `Merge` table: per-step stats must be printed.
select
    countIf(explain like '%I/O: rows%') >= 1,
    countIf(explain like '%parallelism%') >= 1,
    countIf(explain like '%Query summary:%') = 1
from (explain analyze select count() from test_ea_compact_merge where value = 100);

drop table test_ea_compact_mt;
drop table test_ea_compact_merge;
