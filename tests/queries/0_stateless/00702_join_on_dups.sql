drop table if exists X;
drop table if exists Y;

create table X (id Int32, x_a String, x_b Nullable(Int32)) engine Memory;
create table Y (id Int32, y_a String, y_b Nullable(String)) engine Memory;

insert into X (id, x_a, x_b) values (1, 'l1', 1), (2, 'l2', 2), (2, 'l3', 3), (3, 'l4', 4);
insert into X (id, x_a) values      (4, 'l5'), (4, 'l6'), (5, 'l7'), (8, 'l8'), (9, 'l9');
insert into Y (id, y_a) values      (1, 'r1'), (1, 'r2'), (2, 'r3'), (3, 'r4'), (3, 'r5');
insert into Y (id, y_a, y_b) values (4, 'r6', 'nr6'), (6, 'r7', 'nr7'), (7, 'r8', 'nr8'), (9, 'r9', 'nr9');

select 'inner';
select X.*, Y.* from X inner join Y on X.id = Y.id order by X.id, X.x_a, X.x_b, Y.id, Y.y_a, Y.y_b;
select 'inner subs';
select s.*, j.* from (select * from X) as s inner join (select * from Y) as j on s.id = j.id order by s.id, s.x_a, s.x_b, j.id, j.y_a, j.y_b;
select 'inner expr';
select X.*, Y.* from X inner join Y on (X.id + 1) = (Y.id + 1) order by X.id, X.x_a, X.x_b, Y.id, Y.y_a, Y.y_b;

select 'left';
select X.*, Y.* from X left join Y on X.id = Y.id order by X.id, X.x_a, X.x_b, Y.id, Y.y_a, Y.y_b;
select 'left subs';
select s.*, j.* from (select * from X) as s left join (select * from Y) as j on s.id = j.id order by s.id, s.x_a, s.x_b, j.id, j.y_a, j.y_b;
select 'left expr';
select X.*, Y.* from X left join Y on (X.id + 1) = (Y.id + 1) order by X.id, X.x_a, X.x_b, Y.id, Y.y_a, Y.y_b;

select 'right';
select X.*, Y.* from X right join Y on X.id = Y.id order by X.id, X.x_a, X.x_b, Y.id, Y.y_a, Y.y_b;
select 'right subs';
select s.*, j.* from (select * from X) as s right join (select * from Y) as j on s.id = j.id order by s.id, s.x_a, s.x_b, j.id, j.y_a, j.y_b;
--select 'right expr';
--select X.*, Y.* from X right join Y on (X.id + 1) = (Y.id + 1) order by id;

select 'full';
select X.*, Y.* from X full join Y on X.id = Y.id order by X.id, X.x_a, X.x_b, Y.id, Y.y_a, Y.y_b;
select 'full subs';
select s.*, j.* from (select * from X) as s full join (select * from Y) as j on s.id = j.id order by s.id, s.x_a, s.x_b, j.id, j.y_a, j.y_b;
--select 'full expr';
--select X.*, Y.* from X full join Y on (X.id + 1) = (Y.id + 1) order by id;

select 'self inner';
select X.*, s.* from X inner join (select * from X) as s on X.id = s.id order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self inner nullable';
select X.*, s.* from X inner join (select * from X) as s on X.x_b = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self inner nullable vs not nullable';
select X.*, s.* from X inner join (select * from X) as s on X.id = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
-- TODO: s.y_b == '' instead of NULL
select 'self inner nullable vs not nullable 2';
select Y.*, s.* from Y inner join (select * from Y) as s on concat('n', Y.y_a) = s.y_b order by Y.id, Y.y_a, Y.y_b, s.id, s.y_a, s.y_b;

select 'self left';
select X.*, s.* from X left join (select * from X) as s on X.id = s.id order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self left nullable';
select X.*, s.* from X left join (select * from X) as s on X.x_b = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self left nullable vs not nullable';
select X.*, s.* from X left join (select * from X) as s on X.id = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
-- TODO: s.y_b == '' instead of NULL
select 'self left nullable vs not nullable 2';
select Y.*, s.* from Y left join (select * from Y) as s on concat('n', Y.y_a) = s.y_b order by Y.id, Y.y_a, Y.y_b, s.id, s.y_a, s.y_b;

select 'self right';
select X.*, s.* from X right join (select * from X) as s on X.id = s.id order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self right nullable';
select X.*, s.* from X right join (select * from X) as s on X.x_b = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self right nullable vs not nullable';
select X.*, s.* from X right join (select * from X) as s on X.id = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
--select 'self right nullable vs not nullable 2';
--select Y.*, s.* from Y right join (select * from Y) as s on concat('n', Y.y_a) = s.y_b order by id;

select 'self full';
select X.*, s.* from X full join (select * from X) as s on X.id = s.id order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self full nullable';
select X.*, s.* from X full join (select * from X) as s on X.x_b = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
select 'self full nullable vs not nullable';
select X.*, s.* from X full join (select * from X) as s on X.id = s.x_b order by X.id, X.x_a, X.x_b, s.id, s.x_a, s.x_b;
--select 'self full nullable vs not nullable 2';
--select Y.*, s.* from Y full join (select * from Y) as s on concat('n', Y.y_a) = s.y_b order by id;

drop table X;
drop table Y;
