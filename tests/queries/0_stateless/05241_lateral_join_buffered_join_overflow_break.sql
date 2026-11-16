-- A buffered JOIN LATERAL (correlated_subqueries_use_in_memory_buffer = 1) must not let
-- join_overflow_mode = 'break' stop the build side early: the size limit is enforced with THROW.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;
SET correlated_subqueries_use_in_memory_buffer = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (id UInt32, outer_id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO outer_t SELECT number FROM numbers(100);
INSERT INTO inner_t SELECT number, number % 100 FROM numbers(300);

SELECT '-- limit not reached';
SELECT count(), sum(o.id), sum(l.cnt)
FROM outer_t o
INNER JOIN LATERAL (SELECT count() AS cnt FROM inner_t i WHERE i.outer_id = o.id) AS l ON true
SETTINGS max_rows_in_join = 1000000, join_overflow_mode = 'break';

SELECT '-- limit reached, INNER';
SELECT count(), sum(o.id), sum(l.cnt)
FROM outer_t o
INNER JOIN LATERAL (SELECT count() AS cnt FROM inner_t i WHERE i.outer_id = o.id) AS l ON true
SETTINGS max_rows_in_join = 10, join_overflow_mode = 'break'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- limit reached, LEFT';
SELECT count(), sum(o.id), sum(l.cnt)
FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS cnt FROM inner_t i WHERE i.outer_id = o.id) AS l ON true
SETTINGS max_bytes_in_join = 100, join_overflow_mode = 'break'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- limit not reached, LEFT';
SELECT count(), sum(o.id), sum(l.cnt)
FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS cnt FROM inner_t i WHERE i.outer_id = o.id) AS l ON true
SETTINGS max_rows_in_join = 1000000, join_overflow_mode = 'break';

DROP TABLE outer_t;
DROP TABLE inner_t;
