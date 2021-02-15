-- { echo }
create table values_01564(
    a int,
    constraint c1 check a < 10) engine Memory;

-- client error hint after broken insert values
insert into values_01564 values ('f'); -- { clientError 6 }

insert into values_01564 values ('f'); -- { clientError 6 }
select 1;

insert into values_01564 values ('f'); -- { clientError 6 }
select nonexistent column; -- { serverError 47 }

-- syntax error hint after broken insert values
insert into values_01564 this is bad syntax values ('f'); -- { clientError 62 }

insert into values_01564 this is bad syntax values ('f'); -- { clientError 62 }
select 1;

insert into values_01564 this is bad syntax values ('f'); -- { clientError 62 }
select nonexistent column; -- { serverError 47 }

-- server error hint after broken insert values (violated constraint)
insert into values_01564 values (11); -- { serverError 469 }

insert into values_01564 values (11); -- { serverError 469 }
select 1;

insert into values_01564 values (11); -- { serverError 469 }
select nonexistent column; -- { serverError 47 }

-- query after values on the same line
insert into values_01564 values (1); select 1;

-- even this works (not sure why we need it lol)
-- insert into values_01564 values (11) /*{ serverError 469 }*/; select 1;

-- syntax error, where the last token we can parse is long before the semicolon.
select this is too many words for an alias; -- { clientError 62 }
OPTIMIZE TABLE values_01564 DEDUPLICATE BY; -- { clientError 62 }
OPTIMIZE TABLE values_01564 DEDUPLICATE BY a EXCEPT a; -- { clientError 62 }
select 'a' || distinct one || 'c' from system.one; -- { clientError 62 }

-- a failing insert and then a normal insert (#https://github.com/ClickHouse/ClickHouse/issues/19353)
CREATE TABLE t0 (c0 String, c1 Int32) ENGINE = Memory() ;
INSERT INTO t0(c0, c1) VALUES ("1",1) ; -- { clientError 47 }
INSERT INTO t0(c0, c1) VALUES ('1', 1) ;

-- the return code must be zero after the final query has failed with expected error
insert into values_01564 values (11); -- { serverError 469 }

drop table t0;
drop table values_01564;
