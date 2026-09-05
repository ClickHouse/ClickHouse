-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: Depends on AWS

-- The `deletes_db/eq_deletes_table` fixture has 1010 data rows, 30 position deletes and
-- equality deletes that remove 99 more rows, leaving 881 live rows. Its snapshot summary
-- claims `total-records = 1010`. An equality delete file records delete predicates, not
-- the number of data rows they match, so no metadata can produce an exact count: the
-- trivial count optimization must NOT be applied and count() must fall back to a real
-- scan that applies the equality delete transformers.

-- Must return the live row count, not 1010 (summary) and not 980 (data rows minus
-- position deletes only). `optimize_count_from_files` is pinned ON: the count-only fast
-- path feeds synthetic default-valued chunks to the equality-delete FilterTransform (its
-- predicate then matches the wrong rows), so it must be demoted to a real read for files
-- with attached deletes -- with a regression there this query returns 1010, not 881.
-- The settings are pinned because the test runner randomizes them.
SELECT count() FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')
    SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;

-- Sanity check: same result as a full scan.
SELECT count() FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')
    SETTINGS optimize_trivial_count_query = 0, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;

-- The trivial count optimization must not be applied when equality deletes are present,
-- even when explicitly enabled.
SELECT count() FROM (EXPLAIN SELECT count() FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table') SETTINGS optimize_trivial_count_query = 1) WHERE explain LIKE '%Optimized trivial count%';
