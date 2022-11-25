drop table if exists t;
create table t (x Bool) engine=Memory();
insert into t values settings bool_true_representation='да' ('да');
drop table t;
