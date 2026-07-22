select * from numbers(1) t1 left outer join numbers(1) t2 using number;
select * from numbers(1) t1 right outer join numbers(1) t2 using number;

select * from numbers(1) t1 left any join numbers(1) t2 using number;
select * from numbers(1) t1 right any join numbers(1) t2 using number;

select * from numbers(1) t1 left semi join numbers(1) t2 using number;
select * from numbers(1) t1 right semi join numbers(1) t2 using number;

select * from numbers(1) t1 left anti join numbers(1) t2 using number;
select * from numbers(1) t1 right anti join numbers(1) t2 using number;

select * from numbers(1) t1 asof join numbers(1) t2 using number; -- { serverError SYNTAX_ERROR }
select * from numbers(1) t1 left asof join numbers(1) t2 using number; -- { serverError SYNTAX_ERROR }

-- legacy

select * from numbers(1) t1 all left join numbers(1) t2 using number;
select * from numbers(1) t1 all right join numbers(1) t2 using number;

select * from numbers(1) t1 any left join numbers(1) t2 using number;
select * from numbers(1) t1 any right join numbers(1) t2 using number;

select * from numbers(1) t1 semi left join numbers(1) t2 using number;
select * from numbers(1) t1 semi right join numbers(1) t2 using number;

select * from numbers(1) t1 anti left join numbers(1) t2 using number;
select * from numbers(1) t1 anti right join numbers(1) t2 using number;

select * from numbers(1) t1 asof join numbers(1) t2 using number; -- { serverError SYNTAX_ERROR }
select * from numbers(1) t1 asof left join numbers(1) t2 using number; -- { serverError SYNTAX_ERROR }
