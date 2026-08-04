-- The internal decorrelation join must run even when join_algorithm excludes hash joins.
-- See https://github.com/ClickHouse/ClickHouse/issues/103294

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET correlated_subqueries_use_in_memory_buffer = 1;

CREATE TABLE tbl (number UInt64) ENGINE = Memory;
INSERT INTO tbl SELECT number FROM numbers(2);

SELECT 'auto';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'auto';

SELECT 'grace_hash';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'grace_hash';

SELECT 'partial_merge';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'partial_merge';

SELECT 'full_sorting_merge';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'grace_hash,full_sorting_merge';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'grace_hash,full_sorting_merge';

-- hash-family algorithms were never affected and keep working.
SELECT 'hash';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'hash';

SELECT 'parallel_hash';
SELECT number FROM tbl AS t WHERE exists((SELECT number FROM numbers(2) WHERE number >= t.number)) ORDER BY number SETTINGS join_algorithm = 'parallel_hash';

DROP TABLE tbl;
