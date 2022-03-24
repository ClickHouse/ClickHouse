DROP TABLE IF EXISTS test_tuple_filter;

CREATE TABLE test_tuple_filter (id UInt32, value String, log_date Date) Engine=MergeTree() ORDER BY id PARTITION BY log_date SETTINGS index_granularity = 3;

INSERT INTO test_tuple_filter VALUES (1,'A','2021-01-01'),(2,'B','2021-01-01'),(3,'C','2021-01-01'),(4,'D','2021-01-02'),(5,'E','2021-01-02');

SET force_primary_key = 1;
SET optimize_move_to_prewhere = 1;

SELECT * FROM test_tuple_filter WHERE (id, value) = (1, 'A');
SELECT * FROM test_tuple_filter WHERE (1, 'A') = (id, value);
SELECT * FROM test_tuple_filter WHERE (id, value) = (1, 'A') AND (id, log_date) = (1, '2021-01-01');
SELECT * FROM test_tuple_filter WHERE ((id, value), id * 2) = ((1, 'A'), 2);
SELECT * FROM test_tuple_filter WHERE ((id, value), log_date) = ((1, 'A'), '2021-01-01');

-- not supported functions (concat) do not lost
SELECT * FROM test_tuple_filter WHERE (id, value, value||'foo') = ('1', 'A', 'A');

-- Condition fully moved to PREWHERE and such conditions does not supported yet.
SELECT * FROM test_tuple_filter WHERE (1, (1, (1, (1, (id, value))))) = (1, (1, (1, (1, (1, 'A'))))); -- { serverError INDEX_NOT_USED }

-- not implemented yet
SELECT * FROM test_tuple_filter WHERE (1, value) = (id, 'A'); -- { serverError INDEX_NOT_USED }
SELECT * FROM test_tuple_filter WHERE (1, (1, (1, (1, tuple(id))))) = (1, (1, (1, (1, tuple(1))))); -- { serverError INDEX_NOT_USED }
SELECT * FROM test_tuple_filter WHERE ((id, value), tuple(log_date)) = ((1, 'A'), tuple('2021-01-01')); -- { serverError INDEX_NOT_USED }

SET force_index_by_date = 1;
SET force_primary_key = 0;
SELECT * FROM test_tuple_filter WHERE (log_date, value) = ('2021-01-01', 'A');

SET force_index_by_date = 0;
SET force_primary_key = 0;

SELECT * FROM test_tuple_filter WHERE (1, value) = (id, 'A');
SELECT * FROM test_tuple_filter WHERE tuple(id) = tuple(1);

SELECT * FROM test_tuple_filter WHERE (log_date, value) = tuple('2021-01-01'); -- { serverError 43 }
SELECT * FROM test_tuple_filter WHERE (id, value) = tuple(1); -- { serverError 43 }
SELECT * FROM test_tuple_filter WHERE tuple(id, value) = tuple(value, id); -- { serverError 386 }
SELECT * FROM test_tuple_filter WHERE equals((id, value)); -- { serverError 42 }

DROP TABLE IF EXISTS test_tuple_filter;
