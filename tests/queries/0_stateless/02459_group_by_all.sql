DROP TABLE IF EXISTS group_by_all;

CREATE TABLE group_by_all
(
    a String,
    b int
)
engine = Memory;

insert into group_by_all values ('abc1', 0), ('abc2', 0), ('abc3', 0), ('abc4', 0);

select a, count(b) from group_by_all group by all;
select substring(a, 1, 3), count(b) from group_by_all group by all;
select substring(a, 1, 3), substring(substring(a, 1, 2), 1, count(b)) from group_by_all group by all;
select substring(a, 1, 3), substring(a, 1, count(b)) from group_by_all group by all;
