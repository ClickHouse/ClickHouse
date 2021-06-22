DROP TABLE IF EXISTS join_test;

CREATE TABLE join_test (id UInt16, num UInt16) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

CREATE TABLE join_test (id UInt16, num Nullable(UInt16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

CREATE TABLE join_test (id UInt16, num Array(UInt16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500); -- { serverError 43 }
DROP TABLE join_test;

drop table if exists test;
create table test (x Date, y String) engine Join(ANY, LEFT, x);
insert into test values ('2017-04-01', '1396-01-12') ,('2017-04-02', '1396-01-13');

SELECT joinGetOrNull('test', 'y', toDate(A.TVV) ) TV1
FROM ( SELECT rowNumberInAllBlocks() AS R, addDays(toDate('2017-04-01'), R) AS TVV FROM numbers(5) ) AS A
LEFT JOIN ( SELECT rowNumberInAllBlocks() AS R, toDateTime(NULL) AS TVV FROM numbers(1) ) AS B
USING (R) order by TV1;
