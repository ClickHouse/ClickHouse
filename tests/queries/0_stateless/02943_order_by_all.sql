DROP TABLE IF EXISTS order_by_all;

CREATE TABLE order_by_all
(
    a String,
    b int,
    c int
)
engine = Memory;

insert into order_by_all values ('abc2', 3, 2), ('abc3', 2, 3), ('abc2', 1, 1), ('abc1', 3, 2);

select a, b, c from order_by_all order by all;
select count(b), a, count(c) from order_by_all group by all order by all;
select substring(a, 1, 3), b, c from order_by_all order by all;

