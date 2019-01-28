drop table if exists test.persons;
drop table if exists test.children;

create table test.persons (
    id String,
    name String
) engine MergeTree order by id;

create table test.children (
    id String,
    childName String
) engine MergeTree order by id;

insert into test.persons (id, name) values ('1', 'John'), ('2', 'Jack'), ('3', 'Daniel'), ('4', 'James'), ('5', 'Amanda');
insert into test.children (id, childName) values ('1', 'Robert'), ('1', 'Susan'), ('3', 'Sarah'), ('4', 'David'), ('4', 'Joseph'), ('5', 'Robert');


select * from test.persons all inner join test.children using id;

select * from test.persons all inner join (select * from test.children) as j using id;

select * from (select * from test.persons) as s all inner join (select * from test.children) as j using id;


