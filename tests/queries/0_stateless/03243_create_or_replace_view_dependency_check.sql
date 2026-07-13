drop table if exists test;
drop view if exists v;
drop dictionary if exists dict;
create table test (x UInt32, v String) engine=Memory;
create view v (x UInt32, v String) as select x, v from test;
CREATE DICTIONARY dict
(
    x UInt64,
    v String
)
PRIMARY KEY x
SOURCE(CLICKHOUSE(TABLE 'v'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

drop view v; -- {serverError HAVE_DEPENDENT_OBJECTS}
create or replace view v (x UInt32, v String, y UInt32) as select x, v, 42 as y from test;
drop dictionary dict;
drop view v;
drop table test;

