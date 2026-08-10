SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;

CREATE TABLE t_text (x Nullable(String), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_text SELECT if(number % 100 = 7, NULL, 'word' || toString(number)) FROM numbers(1000);

CREATE TABLE t_tokenbf (a Nullable(String), b String, INDEX i b TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tokenbf SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_ngrambf (a Nullable(String), b String, INDEX i b TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ngrambf SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

SELECT 'text', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'text global', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 global', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1 global', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- Pruning the wrong granules would keep the counts above but change the rows, so compare the two
-- settings on each family.
SELECT 'text rows', (SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 rows', (SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);
SELECT 'ngrambf_v1 rows', (SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

-- `globalIn` was admitted but never mapped to an RPN function, so it did not prune even at
-- transform_null_in = 0.
SELECT 'tokenbf_v1 global, transform_null_in = 0', trim(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%';

-- A NULL in the set matches the column's NULL rows, which the indexes cannot express, so the index
-- must be refused and all NULL rows returned.
SELECT count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'text null in set', count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1;
SELECT 'text null in set, transform_null_in = 0', count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 0;

DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
