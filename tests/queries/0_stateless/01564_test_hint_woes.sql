-- { echo }
create table values_01564(
    a int,
    constraint c1 check a < 10) engine Memory;

-- client error hint after broken insert values
insert into values_01564 values ('f'); -- { error CANNOT_PARSE_TEXT }

insert into values_01564 values ('f'); -- { error CANNOT_PARSE_TEXT }
select 1;

insert into values_01564 values ('f'); -- { error CANNOT_PARSE_TEXT }
select nonexistent column; -- { serverError UNKNOWN_IDENTIFIER }

-- syntax error hint after broken insert values
insert into values_01564 this is bad syntax values ('f'); -- { clientError SYNTAX_ERROR }

insert into values_01564 this is bad syntax values ('f'); -- { clientError SYNTAX_ERROR }
select 1;

insert into values_01564 this is bad syntax values ('f'); -- { clientError SYNTAX_ERROR }
select nonexistent column; -- { serverError UNKNOWN_IDENTIFIER }

-- server error hint after broken insert values (violated constraint)
insert into values_01564 values (11); -- { serverError VIOLATED_CONSTRAINT }

insert into values_01564 values (11); -- { serverError VIOLATED_CONSTRAINT }
select 1;

insert into values_01564 values (11); -- { serverError VIOLATED_CONSTRAINT }
select nonexistent column; -- { serverError UNKNOWN_IDENTIFIER }

-- query after values on the same line
insert into values_01564 values (1); select 1;

-- even this works (not sure why we need it lol)
-- insert into values_01564 values (11) /*{ serverError VIOLATED_CONSTRAINT }*/; select 1;

-- syntax error, where the last token we can parse is long before the semicolon.
select this is too many words for an alias; -- { clientError SYNTAX_ERROR }
OPTIMIZE TABLE values_01564 DEDUPLICATE BY; -- { clientError SYNTAX_ERROR }
OPTIMIZE TABLE values_01564 DEDUPLICATE BY a EXCEPT a; -- { clientError SYNTAX_ERROR }
select 'a' || distinct one || 'c' from system.one; -- { clientError SYNTAX_ERROR }

-- a failing insert and then a normal insert (#https://github.com/ClickHouse/ClickHouse/issues/19353)
CREATE TABLE t0 (c0 String, c1 Int32) ENGINE = Memory() ;
INSERT INTO t0(c0, c1) VALUES ("1",1) ; -- { error UNKNOWN_IDENTIFIER }
INSERT INTO t0(c0, c1) VALUES ('1', 1) ;

-- the return code must be zero after the final query has failed with expected error
insert into values_01564 values (11); -- { serverError VIOLATED_CONSTRAINT }

drop table t0;
drop table values_01564;
