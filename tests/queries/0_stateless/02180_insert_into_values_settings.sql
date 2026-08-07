drop table if exists t;
create table t (x Bool) engine=Memory();
insert into t settings bool_true_representation='да' values ('да');
drop table t;
