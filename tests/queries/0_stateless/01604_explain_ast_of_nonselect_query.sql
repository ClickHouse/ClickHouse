explain ast; -- { clientError 62 }
explain ast alter table t1 delete where date = today();
explain ast create function double AS  (n) -> 2*n;
explain ast create table tt(a Nullable(TINYINT UNSIGNED), b Nested(c Nullable(INT UNSIGNED), d String), e Tuple(f TINYINT, g String), h Tuple(INT, String)) Engine=Memory;
