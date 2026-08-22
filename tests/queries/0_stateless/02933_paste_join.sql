select * from (SELECT number as a FROM numbers(10)) t1 PASTE JOIN (select number as a from numbers(10)) t2;
select * from (SELECT number as a FROM numbers(10)) t1 PASTE JOIN (select number as a from numbers(10) order by a desc) t2;
create table if not exists test (number UInt64) engine=Memory;
insert into test select number from numbers(6);
insert into test select number from numbers(5);
SELECT * FROM (SELECT 1) t1 PASTE JOIN (SELECT 2) SETTINGS joined_subquery_requires_alias=0;
select * from (SELECT number as a FROM numbers(11)) t1 PASTE JOIN test t2 SETTINGS max_threads=1;
select * from (SELECT number as a FROM numbers(11)) t1 PASTE JOIN (select * from test limit 2) t2 SETTINGs max_threads=1;
CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = Memory;
INSERT INTO t1 SELECT number, number FROM numbers(0, 3);
INSERT INTO t1 SELECT number, number FROM numbers(3, 2);
INSERT INTO t1 SELECT number, number FROM numbers(5, 7);
INSERT INTO t1 SELECT number, number FROM numbers(12, 2);
INSERT INTO t1 SELECT number, number FROM numbers(14, 1);
INSERT INTO t1 SELECT number, number FROM numbers(15, 2);
INSERT INTO t1 SELECT number, number FROM numbers(17, 1);
INSERT INTO t1 SELECT number, number FROM numbers(18, 2);
INSERT INTO t1 SELECT number, number FROM numbers(20, 2);
INSERT INTO t1 SELECT number, number FROM numbers(22, 2);
INSERT INTO t1 SELECT number, number FROM numbers(24, 2);
INSERT INTO t1 SELECT number, number FROM numbers(26, 2);
INSERT INTO t1 SELECT number, number FROM numbers(28, 2);


CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = Memory;
INSERT INTO t2 SELECT number, number FROM numbers(0, 2);
INSERT INTO t2 SELECT number, number FROM numbers(2, 3);
INSERT INTO t2 SELECT number, number FROM numbers(5, 5);
INSERT INTO t2 SELECT number, number FROM numbers(10, 5);
INSERT INTO t2 SELECT number, number FROM numbers(15, 15);

SELECT * FROM ( SELECT * from t1 ) t1 PASTE JOIN ( SELECT * from t2 ) t2 SETTINGS max_threads = 1;
SELECT toTypeName(a) FROM (SELECT number as a FROM numbers(11)) t1 PASTE JOIN (select number as a from numbers(10)) t2 SETTINGS join_use_nulls = 1;
SET max_threads = 2;
select * from (SELECT number as a FROM numbers_mt(10)) t1 PASTE JOIN (select number as a from numbers(10) ORDER BY a DESC) t2 SETTINGS max_block_size=10;
select * from (SELECT number as a FROM numbers(10)) t1 ANY PASTE JOIN (select number as a from numbers(10)) t2; -- { clientError SYNTAX_ERROR }
select * from (SELECT number as a FROM numbers(10)) t1 ALL PASTE JOIN (select number as a from numbers(10)) t2; -- { clientError SYNTAX_ERROR }

TRUNCATE TABLE test;
INSERT INTO test SELECT number from numbers(6);
SELECT * FROM (SELECT number FROM test) PASTE JOIN (SELECT number FROM numbers(6) ORDER BY number) SETTINGS joined_subquery_requires_alias = 0;
SELECT * FROM (SELECT number FROM test PASTE JOIN (Select number FROM numbers(7))) PASTE JOIN (SELECT number FROM numbers(6) PASTE JOIN (SELECT number FROM test)) SETTINGS joined_subquery_requires_alias = 0;
SELECT * FROM (SELECT number FROM test PASTE JOIN (SELECT number FROM test PASTE JOIN (Select number FROM numbers(7)))) PASTE JOIN (SELECT number FROM numbers(6) PASTE JOIN (SELECT number FROM test)) SETTINGS joined_subquery_requires_alias = 0;
SELECT * FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b) PASTE JOIN (SELECT 3 AS c) SETTINGS enable_analyzer = 1;
SELECT * FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b) PASTE JOIN (SELECT 3 AS a) SETTINGS enable_analyzer = 1; -- { serverError AMBIGUOUS_COLUMN_NAME }

SET enable_analyzer = 1;
CREATE TABLE test1 (a Int32) engine=MergeTree order by a;
INSERT INTO test1 SELECT * FROM numbers(2);
CREATE TABLE test2 (a Int32) engine=MergeTree order by a;
INSERT INTO test2 SELECT * FROM numbers(2);
SELECT * FROM test1 PASTE JOIN (SELECT * FROM test2);
SELECT a `test2.a` FROM test1 PASTE JOIN test2;
SELECT * FROM test1 `test2.a` PASTE JOIN test2 `test2.a`; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM test1 PASTE JOIN (SELECT number AS a FROM numbers(2) ORDER BY number DESC); -- { serverError AMBIGUOUS_COLUMN_NAME }
