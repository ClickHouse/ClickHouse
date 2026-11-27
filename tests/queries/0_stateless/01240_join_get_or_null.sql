DROP TABLE IF EXISTS join_test;

CREATE TABLE join_test (id UInt16, num UInt16) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

CREATE TABLE join_test (id UInt16, num Nullable(UInt16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

CREATE TABLE join_test (id UInt16, num Array(UInt16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP TABLE join_test;

drop table if exists test;
create table test (x Date, y String) engine Join(ANY, LEFT, x);
insert into test values ('2017-04-01', '1396-01-12') ,('2017-04-02', '1396-01-13');

WITH
    A as (SELECT rowNumberInAllBlocks() R, addDays(toDate('2017-04-01'), R) TVV from numbers(5)),
    B as (SELECT rowNumberInAllBlocks() R, toDateTime(NULL) TVV from numbers(1))
SELECT
    joinGetOrNull('test', 'y', toDate(A.TVV) ) TV1
from A LEFT JOIN B USING (R) order by TV1;
