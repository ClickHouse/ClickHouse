create table test1 (id UInt8) engine = TinyLog;
create table test2 (id UInt8) engine = TinyLog;

create view v_test1 as select id from test1;
create view v_test2 as select id from test2;

rename table v_test1 to v_test11, v_test2 to v_test22;

SELECT name, engine FROM system.tables WHERE name IN ('v_test1', 'v_test2', 'v_test11', 'v_test22') AND database = 'default' ORDER BY name;
