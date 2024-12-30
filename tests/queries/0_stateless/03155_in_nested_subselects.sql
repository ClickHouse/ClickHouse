-- https://github.com/ClickHouse/ClickHouse/issues/63833
SET enable_analyzer = 1;

create table Example (id Int32) engine = MergeTree ORDER BY id;
INSERT INTO Example SELECT number AS id FROM numbers(2);

create table Null engine=Null as Example ;
--create table Null engine=MergeTree order by id as Example ;

create materialized view Transform to Example as
select * from Null
join ( select * FROM Example
       WHERE id IN (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM Null)))))
     ) as old
using id;

INSERT INTO Null SELECT number AS id FROM numbers(2);

select * from Example order by all;  -- should return 4 rows
