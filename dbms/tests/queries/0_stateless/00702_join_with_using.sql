DROP TABLE IF EXISTS test.using1;
DROP TABLE IF EXISTS test.using2;

CREATE TABLE test.using1(a UInt8, b UInt8) ENGINE=Memory;
CREATE TABLE test.using2(a UInt8, b UInt8) ENGINE=Memory;

INSERT INTO test.using1 VALUES (1, 1) (2, 2) (3, 3);
INSERT INTO test.using2 VALUES (4, 4) (2, 2) (3, 3);

SELECT * FROM test.using1 ALL LEFT JOIN (SELECT * FROM test.using2) USING (a, a, a, b, b, b, a, a) ORDER BY a;

DROP TABLE test.using1;
DROP TABLE test.using2;

--

use test;
drop table if exists persons;
drop table if exists children;

create table persons (id String, name String) engine MergeTree order by id;
create table children (id String, childName String) engine MergeTree order by id;

insert into persons (id, name)
values ('1', 'John'), ('2', 'Jack'), ('3', 'Daniel'), ('4', 'James'), ('5', 'Amanda');

insert into children (id, childName)
values ('1', 'Robert'), ('1', 'Susan'), ('3', 'Sarah'), ('4', 'David'), ('4', 'Joseph'), ('5', 'Robert');

select * from persons all inner join children using id;
select * from persons all inner join (select * from children) as j using id;
select * from (select * from persons) as s all inner join (select * from children ) as j using id;
--
select * from persons all inner join (select * from children) using id;
select * from (select * from persons) all inner join (select * from children) using id;
select * from (select * from persons) as s all inner join (select * from children) using id;

drop table persons;
drop table children;
