SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_text;
DROP TABLE IF EXISTS t_tokenbf;
DROP TABLE IF EXISTS t_ngrambf;
DROP TABLE IF EXISTS t_null_set;
DROP TABLE IF EXISTS t_free_set;

CREATE TABLE t_text (x Nullable(String), INDEX i x TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_text SELECT if(number % 100 = 7, NULL, 'word' || toString(number)) FROM numbers(1000);

CREATE TABLE t_tokenbf (a Nullable(String), b String, INDEX i b TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_tokenbf SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

CREATE TABLE t_ngrambf (a Nullable(String), b String, INDEX i b TYPE ngrambf_v1(3, 256, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
INSERT INTO t_ngrambf SELECT if(number % 100 = 7, NULL, 'a' || toString(number)), 'word' || toString(number) FROM numbers(1000);

SELECT 'text', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'text global', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_text WHERE x GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 global', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'ngrambf_v1 global', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrambf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

-- Pruning the wrong granules would keep the counts above but change the rows, so compare the two
-- settings on each family.
SELECT 'text rows', (SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_text WHERE x IN ('word5', 'word500') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 rows', (SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tokenbf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);
SELECT 'ngrambf_v1 rows', (SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 0) = (SELECT count() FROM t_ngrambf WHERE (a, b) IN (('a5', 'word5'), ('a500', 'word500')) SETTINGS transform_null_in = 1);

-- `globalIn` was admitted but never mapped to an RPN function, so it did not prune even at
-- transform_null_in = 0.
SELECT 'tokenbf_v1 global, transform_null_in = 0', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b GLOBAL IN ('word5', 'word500') SETTINGS transform_null_in = 0) WHERE explain LIKE '%Granules: %/%';

-- All four `NOT IN` spellings now reach the index. A bare `NOT IN` cannot prune on its own, since
-- `FUNCTION_NOT_IN` is always allowed to be true, so assert it is used rather than that it prunes,
-- and assert pruning under the negation that cancels it. `NOT (x NOT IN ...)` folds to `nullIn`, so
-- the `globalNotNullIn` spelling is the one that exercises the negated branch.
SELECT 'tokenbf_v1 not null in used', count() FROM t_tokenbf WHERE b NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'tokenbf_v1 global not in used', count() FROM t_tokenbf WHERE b GLOBAL NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 0;
SELECT 'tokenbf_v1 global not null in used', count() FROM t_tokenbf WHERE b GLOBAL NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'ngrambf_v1 not null in used', count() FROM t_ngrambf WHERE b NOT IN ('word5', 'word500') SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1;
SELECT 'tokenbf_v1 negated global not null in', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE NOT (b GLOBAL NOT IN ('word5', 'word500')) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 not in rows', (SELECT count() FROM t_tokenbf WHERE b NOT IN ('word5', 'word500') SETTINGS transform_null_in = 0) = (SELECT count() FROM t_tokenbf WHERE b NOT IN ('word5', 'word500') SETTINGS transform_null_in = 1);
SELECT 'tokenbf_v1 negated not in rows', (SELECT count() FROM t_tokenbf WHERE NOT (b GLOBAL NOT IN ('word5', 'word500')) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tokenbf WHERE NOT (b GLOBAL NOT IN ('word5', 'word500')) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- A NULL in the set matches the column's NULL rows, which the indexes cannot express, so the index
-- must be refused and all NULL rows returned.
SELECT count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'text null in set', count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 1;
SELECT 'text null in set, transform_null_in = 0', count() FROM t_text WHERE x IN ('word5', NULL) SETTINGS transform_null_in = 0;

-- The token bloom filter family rejects a `Nullable` index column, so its per-row refusal is
-- reached through a subquery set, whose element type keeps the wrapper.
CREATE TABLE t_null_set (v Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_set VALUES ('word5'), (NULL);

SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS force_data_skipping_indices = 'i', transform_null_in = 1; -- { serverError INDEX_NOT_USED }
SELECT 'tokenbf_v1 null in subquery set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';
SELECT 'tokenbf_v1 null in subquery set rows', (SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);
SELECT 'ngrambf_v1 null in subquery set rows', (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1) = (SELECT count() FROM t_ngrambf WHERE b IN (SELECT v FROM t_null_set) SETTINGS transform_null_in = 1, use_skip_indexes = 0);

-- A null-free subquery set over the same `Nullable` element type must still prune.
CREATE TABLE t_free_set (v Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_free_set VALUES ('word5'), ('word500');
SELECT 'tokenbf_v1 null-free subquery set', extract(explain, 'Granules: \\d+/\\d+') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tokenbf WHERE b IN (SELECT v FROM t_free_set) SETTINGS transform_null_in = 1) WHERE explain LIKE '%Granules: %/%';

DROP TABLE t_text;
DROP TABLE t_tokenbf;
DROP TABLE t_ngrambf;
DROP TABLE t_null_set;
DROP TABLE t_free_set;
