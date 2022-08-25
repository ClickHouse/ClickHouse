select substring('abcdefgh', -2, -2);
select substring(materialize('abcdefgh'), -2, -2);
select substring(materialize('abcdefgh'), materialize(-2), materialize(-2));

select substring('abcdefgh', -2, -1);
select substring(materialize('abcdefgh'), -2, -1);
select substring(materialize('abcdefgh'), materialize(-2), materialize(-1));

select '-';
select substring(cast('abcdefgh' as FixedString(8)), -2, -2);
select substring(materialize(cast('abcdefgh' as FixedString(8))), -2, -2);
select substring(materialize(cast('abcdefgh' as FixedString(8))), materialize(-2), materialize(-2));

select substring(cast('abcdefgh' as FixedString(8)), -2, -1);
select substring(materialize(cast('abcdefgh' as FixedString(8))), -2, -1);
select substring(materialize(cast('abcdefgh' as FixedString(8))), materialize(-2), materialize(-1));

select '-';
drop table if exists t;
create table t
(
    s String,
    l Int8,
    r Int8
) engine = Memory;

insert into t values ('abcdefgh', -2, -2),('12345678', -3, -3);

select substring(s, -2, -2) from t;
select substring(s, l, -2) from t;
select substring(s, -2, r) from t;
select substring(s, l, r) from t;

select '-';
drop table if exists t;
create table t(
                  s FixedString(8),
                  l Int8,
                  r Int8
) engine = Memory;
insert into t values ('abcdefgh', -2, -2),('12345678', -3, -3);

select substring(s, -2, -2) from t;
select substring(s, l, -2) from t;
select substring(s, -2, r) from t;
select substring(s, l, r) from t;

drop table if exists t;
