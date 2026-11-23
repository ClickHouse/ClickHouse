DROP TABLE IF EXISTS group_by_all;

CREATE TABLE group_by_all
(
    a String,
    b int,
    c int
)
engine = Memory;

insert into group_by_all values ('abc1', 1, 1), ('abc2', 1, 1), ('abc3', 1, 1), ('abc4', 1, 1);

select a, count(b) from group_by_all group by all order by a;
select substring(a, 1, 3), count(b) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, 1, 2), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, 1, 2), c, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, c, 2), c, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, c + 1, 2), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, c + 1, 2), c, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(substring(a, c, count(b)), 1, count(b)), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(a, 1, count(b)) from group_by_all group by all;
select count(b) AS len, substring(a, 1, 3), substring(a, 1, len) from group_by_all group by all;

SET enable_analyzer = 1;

select a, count(b) from group_by_all group by all order by a;
select substring(a, 1, 3), count(b) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, 1, 2), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, 1, 2), c, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, c, 2), c, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, c + 1, 2), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, c + 1, 2), c, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(substring(a, c, count(b)), 1, count(b)), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(a, 1, count(b)) from group_by_all group by all;
select count(b) AS len, substring(a, 1, 3), substring(a, 1, len) from group_by_all group by all;
