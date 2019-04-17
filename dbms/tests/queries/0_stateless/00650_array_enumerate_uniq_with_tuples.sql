drop table if exists tab;
create table tab (val UInt32, n Nested(x UInt8, y String)) engine = Memory;
insert into tab values (1, [1, 2, 1, 1, 2, 1], ['a', 'a', 'b', 'a', 'b', 'b']);
select arrayEnumerateUniq(n.x) from tab;
select arrayEnumerateUniq(n.y) from tab;
select arrayEnumerateUniq(n.x, n.y) from tab;
select arrayEnumerateUniq(arrayMap((a, b) -> (a, b), n.x, n.y)) from tab;
select arrayEnumerateUniq(arrayMap((a, b) -> (a, b), n.x, n.y), n.x) from tab;
select arrayEnumerateUniq(arrayMap((a, b) -> (a, b), n.x, n.y), arrayMap((a, b) -> (b, a), n.x, n.y)) from tab;

drop table tab;
create table tab (val UInt32, n Nested(x Nullable(UInt8), y String)) engine = Memory;
insert into tab values (1, [1, Null, 2, 1, 1, 2, 1, Null, Null], ['a', 'a', 'a', 'b', 'a', 'b', 'b', 'b', 'a']);
select arrayEnumerateUniq(n.x) from tab;
select arrayEnumerateUniq(n.y) from tab;
select arrayEnumerateUniq(n.x, n.y) from tab;
select arrayEnumerateUniq(arrayMap((a, b) -> (a, b), n.x, n.y)) from tab;
select arrayEnumerateUniq(arrayMap((a, b) -> (a, b), n.x, n.y), n.x) from tab;
select arrayEnumerateUniq(arrayMap((a, b) -> (a, b), n.x, n.y), arrayMap((a, b) -> (b, a), n.x, n.y)) from tab;

drop table tab;
