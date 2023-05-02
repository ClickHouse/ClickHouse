CREATE TABLE TestTbl(id UInt16, val String)
ENGINE = ReplacingMergeTree
ORDER BY (id) as 
SELECT 1, 'issue';  

CREATE DICTIONARY TestTblDict(id UInt16, val String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE TestTbl DB currentDatabase()))
LIFETIME(1)
LAYOUT(COMPLEX_KEY_HASHED());

select * FROM TestTblDict;

insert into TestTbl values(1, 'no issue');
optimize table TestTbl final;

SELECT sleep(3) format Null;

select * FROM TestTblDict; 

SYSTEM RELOAD DICTIONARY TestTblDict;

select * FROM TestTblDict; 

drop dictionary TestTblDict;
drop table TestTbl;
