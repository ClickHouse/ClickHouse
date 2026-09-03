-- Testing basic functionality with compact parts
set mutations_sync = 2;
drop table if exists mt_compact;

create table mt_compact(a UInt64, b UInt64 DEFAULT a * a, s String, n Nested(x UInt32, y String), lc LowCardinality(String))
engine = MergeTree
order by a partition by a % 10
settings index_granularity = 8,
min_bytes_for_wide_part = 0,
min_rows_for_wide_part = 10;

insert into mt_compact (a, s, n.y, lc) select number, toString((number * 2132214234 + 5434543) % 2133443), ['a', 'b', 'c'], number % 2 ? 'bar' : 'baz' from numbers(90);

select * from mt_compact order by a limit 10;
select '=====================';

select distinct part_type from system.parts where database = currentDatabase() and table = 'mt_compact' and active;

insert into mt_compact (a, s, n.x, lc) select number % 3, toString((number * 75434535 + 645645) % 2133443), [1, 2], toString(number) from numbers(5);

optimize table mt_compact final;

select part_type, count() from system.parts where database = currentDatabase() and table = 'mt_compact' and active group by part_type order by part_type;
select * from mt_compact order by a, s limit 10;
select '=====================';

alter table mt_compact drop column n.y;
alter table mt_compact add column n.y Array(String) DEFAULT ['qwqw'] after n.x;
select * from mt_compact order by a, s limit 10;
select '=====================';

alter table mt_compact update b = 42 where 1;

select * from mt_compact where a > 1 order by a, s limit 10;
select '=====================';

drop table if exists mt_compact;
