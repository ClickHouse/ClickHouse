select * from (SELECT number as a FROM numbers(10)) t1 PASTE JOIN (select number as a from numbers(10)) t2;
select * from (SELECT number as a FROM numbers(10)) t1 PASTE JOIN (select number as a from numbers(10) order by a desc) t2;
create table if not exists test (num UInt64) engine=Memory;
insert into test select number from numbers(6);
insert into test select number from numbers(5);
select * from (SELECT number as a FROM numbers(11)) t1 PASTE JOIN test t2 SETTINGS max_threads=1;
select * from (SELECT number as a FROM numbers(11)) t1 PASTE JOIN (select * from test limit 2) t2 SETTINGs max_threads=1;
select * from (SELECT number as a FROM numbers(10)) t1 ANY PASTE JOIN (select number as a from numbers(10)) t2; -- { clientError SYNTAX_ERROR }
select * from (SELECT number as a FROM numbers(10)) t1 ALL PASTE JOIN (select number as a from numbers(10)) t2; -- { clientError SYNTAX_ERROR }
select * from (SELECT number as a FROM numbers_mt(10)) t1 PASTE JOIN (select number as a from numbers(10) ORDER BY a DESC) t2 SETTINGS max_block_size=3; -- { serverError BAD_ARGUMENTS }
