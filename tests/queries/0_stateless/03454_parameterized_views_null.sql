SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
create view test_table as select * from system.one where dummy={param:Nullable(Int64)} IS NULL;
select * from test_table(param=NULL);
DROP TABLE test_table;
