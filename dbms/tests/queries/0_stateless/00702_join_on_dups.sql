use test;
drop table if exists X;
drop table if exists Y;

create table X (id Int32, x_name String) engine Memory;
create table Y (id Int32, y_name String) engine Memory;

insert into X (id, x_name) values (1, 'A'), (2, 'B'), (2, 'C'), (3, 'D'), (4, 'E'), (4, 'F'), (5, 'G'), (8, 'H'), (9, 'I');
insert into Y (id, y_name) values (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (3, 'e'), (4, 'f'), (6, 'g'), (7, 'h'), (9, 'i');

select 'inner';
select X.*, Y.* from X inner join Y on X.id = Y.id;
select 'inner subs';
select s.*, j.* from (select * from X) as s inner join (select * from Y) as j on s.id = j.id;
select 'inner expr';
select X.*, Y.* from X inner join Y on (X.id + 1) = (Y.id + 1);

select 'left';
select X.*, Y.* from X left join Y on X.id = Y.id;
select 'left subs';
select s.*, j.* from (select * from X) as s left join (select * from Y) as j on s.id = j.id;
select 'left expr';
select X.*, Y.* from X left join Y on (X.id + 1) = (Y.id + 1);

select 'right';
select X.*, Y.* from X right join Y on X.id = Y.id order by id;
select 'right subs';
select s.*, j.* from (select * from X) as s right join (select * from Y) as j on s.id = j.id order by id;
--select 'right expr';
--select X.*, Y.* from X right join Y on (X.id + 1) = (Y.id + 1) order by id;

select 'full';
select X.*, Y.* from X full join Y on X.id = Y.id order by id;
select 'full subs';
select s.*, j.* from (select * from X) as s full join (select * from Y) as j on s.id = j.id order by id;
--select 'full expr';
--select X.*, Y.* from X full join Y on (X.id + 1) = (Y.id + 1) order by id;

drop table X;
drop table Y;
