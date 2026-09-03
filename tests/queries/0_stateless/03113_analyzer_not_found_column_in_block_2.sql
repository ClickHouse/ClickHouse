-- https://github.com/ClickHouse/ClickHouse/pull/62457

drop table if exists t;

create table t  (ID String) Engine= Memory() ;
insert into t values('a'),('b'),('c');

-- This optimization is disabled by default and even its description says that it could lead to
-- inconsistencies for distributed queries.
set optimize_if_transform_strings_to_enum=0;
set enable_analyzer=1;

SELECT multiIf( ((multiIf(ID='' AND (ID = 'a' OR ID = 'c' OR ID = 'b'),'a','x') as y) = 'c') OR
                (multiIf(ID='' AND (ID = 'a' OR ID = 'c' OR ID = 'b'),'a','x') = 'b') OR
                (multiIf(ID='' AND (ID = 'a' OR ID = 'c' OR ID = 'b'),'a','x') = 'd') OR
                (multiIf(ID='' AND (ID = 'a' OR ID = 'c' OR ID = 'b'),'a','x') = 'e'),'test', 'x'
       ) AS alias
FROM remote('127.0.0.{1,2}', currentDatabase(), t)
GROUP BY alias;

drop table if exists t;
