set output_format_pretty_color=1;

-- Disable external aggregation because it may produce several blocks instead of one.
set max_bytes_before_external_group_by = 0;
set max_bytes_ratio_before_external_group_by = 0;
set output_format_write_statistics = 0;

select g, s from (select g, sum(number) as s from numbers(4) group by bitAnd(number, 1) as g with totals order by g) array join [1, 2] as a format Pretty;
select '--';

select g, s from (select g, sum(number) as s from numbers(4) group by bitAnd(number, 1) as g with totals order by g) array join [1, 2] as a format TSV;
select '--';

select g, s from (select g, sum(number) as s from numbers(4) group by bitAnd(number, 1) as g with totals order by g) array join [1, 2] as a format JSON;
select '--';
