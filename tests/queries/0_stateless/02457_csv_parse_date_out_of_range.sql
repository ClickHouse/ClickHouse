DROP TABLE IF EXISTS test_date_out_of_range sync;
CREATE TABLE test_date_out_of_range (f String, t Date) engine=Memory();
INSERT INTO test_date_out_of_range format CSV 'above',2200-12-31
INSERT INTO test_date_out_of_range format CSV 'below',1900-01-01
SELECT * from test_date_out_of_range;
DROP TABLE test_date_out_of_range SYNC;
