select arraySlice([1, 2, 3, 4, 5, 6, 7, 8], -2, -2);
select arraySlice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), -2, -2);
select arraySlice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), materialize(-2), materialize(-2));

select arraySlice([1, 2, 3, 4, 5, 6, 7, 8], -2, -1);
select arraySlice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), -2, -1);
select arraySlice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), materialize(-2), materialize(-1));

select '-';
drop table if exists t;
create table t
(
    s Array(Int),
    l Int8,
    r Int8
) engine = Memory;

insert into t values ([1, 2, 3, 4, 5, 6, 7, 8], -2, -2), ([1, 2, 3, 4, 5, 6, 7, 8], -3, -3);

select arraySlice(s, -2, -2) from t;
select arraySlice(s, l, -2) from t;
select arraySlice(s, -2, r) from t;
select arraySlice(s, l, r) from t;

drop table t;
