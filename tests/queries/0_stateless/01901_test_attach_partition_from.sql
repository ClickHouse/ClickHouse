DROP TABLE IF EXISTS test_alter_attach_01901S;
DROP TABLE IF EXISTS test_alter_attach_01901D;

CREATE TABLE test_alter_attach_01901S (A Int64, D date) ENGINE = MergeTree partition by D order by A;
insert into test_alter_attach_01901S values(1, '2020-01-01');

create table test_alter_attach_01901D (A Int64, D date) 
Engine=ReplicatedMergeTree('/clickhouse/tables/test_alter_attach_01901D', 'r1')
partition by D order by A;

alter table test_alter_attach_01901D attach partition '2020-01-01' from test_alter_attach_01901S;

select count() from test_alter_attach_01901D;
select count() from test_alter_attach_01901S;

insert into test_alter_attach_01901S values(1, '2020-01-01');
alter table test_alter_attach_01901D replace partition '2020-01-01' from test_alter_attach_01901S;

select count() from test_alter_attach_01901D;
select count() from test_alter_attach_01901S;

DROP TABLE test_alter_attach_01901S;
DROP TABLE test_alter_attach_01901D;

