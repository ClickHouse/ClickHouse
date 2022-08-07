-- Tags: no-replicated-database
-- Tag no-replicated-database: Does not support renaming of multiple tables in single query

DROP TABLE IF EXISTS test1_00634;
DROP TABLE IF EXISTS test2_00634;
DROP TABLE IF EXISTS v_test1;
DROP TABLE IF EXISTS v_test2;
DROP TABLE IF EXISTS v_test11;
DROP TABLE IF EXISTS v_test22;

create table test1_00634 (id UInt8) engine = TinyLog;
create table test2_00634 (id UInt8) engine = TinyLog;

create view v_test1 as select id from test1_00634;
create view v_test2 as select id from test2_00634;

rename table v_test1 to v_test11, v_test2 to v_test22;

SELECT name, engine FROM system.tables WHERE name IN ('v_test1', 'v_test2', 'v_test11', 'v_test22') AND database = currentDatabase() ORDER BY name;

DROP TABLE v_test11;
DROP TABLE v_test22;
DROP TABLE test1_00634;
DROP TABLE test2_00634;
