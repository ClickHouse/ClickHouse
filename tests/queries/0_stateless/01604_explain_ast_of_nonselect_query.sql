explain ast; -- { clientError SYNTAX_ERROR }
explain ast alter table t1 delete where date = today();
explain ast create function double AS  (n) -> 2*n;
