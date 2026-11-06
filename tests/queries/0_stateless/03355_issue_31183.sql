create table test1(col UInt64, col_sq UInt64 MATERIALIZED col*col) Engine=MergeTree partition by tuple() order by tuple();
insert into test1 values (1),(2);

create table test2(col UInt64) Engine=MergeTree partition by tuple() order by tuple();
insert into test2 values (1),(2);

SELECT t1.col, t1.col_sq
FROM test2 t2
LEFT JOIN test1 t1 ON t1.col = t2.col 
SETTINGS enable_analyzer=1;

SELECT t1.col, t1.col_sq
FROM test2 t2
LEFT JOIN test1 t1 ON t1.col = t2.col 
SETTINGS enable_analyzer=0; -- {serverError UNKNOWN_IDENTIFIER}

