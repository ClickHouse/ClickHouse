


select * from ( select sum(last_seen) as dates_seen, materialize(1) as last_seen ) where last_seen > 2;
select * from ( select sum(last_seen) as dates_seen, materialize(2) as last_seen ) where last_seen < 2;
select * from ( select sum(last_seen) as dates_seen, materialize(2) as last_seen GROUP BY 'a' ) where last_seen < 2;

select '---';
select * from ( select sum(last_seen) as dates_seen, 1 as last_seen UNION ALL select sum(last_seen) as dates_seen, 3 as last_seen ) where last_seen < 2;

select '---';
select * from ( select sum(last_seen) as dates_seen, 1 as last_seen UNION ALL select sum(last_seen) as dates_seen, 3 as last_seen ) where last_seen > 2;

select '---';
with activity as (
  select
    groupUniqArrayState(toDate('2025-01-01 01:00:00')) as dates_seen,
    toDateTime('2025-01-01 01:00:00') as last_seen
  union all
  select
    groupUniqArrayState(toDate('2023-11-11 11:11:11')) as dates_seen,
    toDateTime('2023-11-11 11:11:11') as last_seen
)
select last_seen from activity
where last_seen < toDateTime('2020-01-01 00:00:00');
select '---';
