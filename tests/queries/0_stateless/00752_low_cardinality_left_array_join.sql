drop table if exists lc_left_aj;
CREATE TABLE lc_left_aj
(
    str Array(LowCardinality(String)), 
    null_str Array(LowCardinality(Nullable(String))), 
    val Array(LowCardinality(Float64)), 
    null_val Array(LowCardinality(Nullable(Float64)))
)
ENGINE = Memory;

insert into lc_left_aj values (['a', 'b'], ['c', Null], [1, 2.0], [3., Null]), ([], ['c', Null], [1, 2.0], [3., Null]), (['a', 'b'], [], [1, 2.0], [3., Null]), (['a', 'b'], ['c', Null], [], [3., Null]), (['a', 'b'], ['c', Null], [1, 2.0], []);

select *, arr from lc_left_aj left array join str as arr;
select '-';
select *, arr from lc_left_aj left array join null_str as arr;
select '-';
select *, arr from lc_left_aj left array join val as arr;
select '-';
select *, arr from lc_left_aj left array join null_val as arr;
drop table if exists lc_left_aj;

