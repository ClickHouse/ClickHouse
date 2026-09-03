create table t (a String, b String, c String, d String) order by (a, b, c, d) settings index_granularity=10;
insert into t select intDiv(number, 50), intDiv(number, 50), 0, number % 10 from numbers(50 + 10);
select count() from t where a = '0' and b = '0' and (c, d) in ('0', '5');
select count() from t where a = '0' and b = '0' and (c, d) in ('0', '5') settings optimize_use_implicit_projections=0;

-- Have some granules where all rows pass the filter.
insert into t select intDiv(number, 100), intDiv(number, 100), 0, intDiv(number, 33) from numbers(100 + 10);
select count() from t where a = '0' and b = '0' and (c, d) in ('0', '2');
select count() from t where a = '0' and b = '0' and (c, d) in ('0', '2') settings optimize_use_implicit_projections=0;
