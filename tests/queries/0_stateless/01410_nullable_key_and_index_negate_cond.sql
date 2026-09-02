drop table if exists test_23634;

set force_primary_key=1;

CREATE TABLE test_23634 (id Nullable(String), s Nullable(String), s1 Nullable(String))
ENGINE = MergeTree() ORDER BY (id,s) SETTINGS allow_nullable_key = 1;

INSERT into test_23634 values ('s','s','s'), (null,'s1','s1'), (null,null,'s2'), (null,null,null);

select '---Q1---';
select * from test_23634 where id !='';

select '---Q2---';
select * from test_23634 where id !='' and s != '';

select '---Q3---';
select * from test_23634 where id !='' and s != '' and s1 != '';

set force_primary_key=0;

select '---Q4---';
select * from test_23634 where (id, s, s1) != ('', '', '') order by id, s1, s1;

select '---Q5---';
select * from test_23634 where (id, s, s1) = ('', '', '') order by id, s1, s1;

select '---Q6---';
select * from test_23634 where (id, s, s1) = ('', '', 's2') order by id, s1, s1;

select '---Q7---';
select * from test_23634 where (id, s, s1) = ('', 's1', 's1') order by id, s1, s1;

select '---Q8---';
select * from test_23634 where (id, s, s1) = ('s', 's', 's') order by id, s1, s1;

select '---Q9---';
select * from test_23634 where (id, s, s1) = (null::Nullable(String), null::Nullable(String), null::Nullable(String)) order by id, s1, s1;

drop table test_23634;

