DROP TABLE IF EXISTS test_tuple_filter;

CREATE TABLE test_tuple_filter (id UInt32, value String, log_date Date) Engine=MergeTree() ORDER BY id PARTITION BY log_date settings index_granularity=3;

insert into test_tuple_filter values (1, 'A','2021-01-01'),(2,'B','2021-01-01'),(3,'C','2021-01-01'),(4,'D','2021-01-02'),(5,'E','2021-01-02');

set force_primary_key=1;
SELECT * FROM test_tuple_filter WHERE (id, value) = (1, 'A');

set force_index_by_date=1;
set force_primary_key=0;
SELECT * FROM test_tuple_filter WHERE (log_date, value) = ('2021-01-01', 'A');

DROP TABLE IF EXISTS test_tuple_filter;
