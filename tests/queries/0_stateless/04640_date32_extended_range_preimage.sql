-- The preimage optimization (rewriting `toYear(d) = N` into a raw range on `d`) covers the extended Date32 range.
-- https://github.com/ClickHouse/ClickHouse/issues/111524

SET session_timezone = 'UTC';
SET optimize_time_filter_with_preimage = 1;

DROP TABLE IF EXISTS date32_preimage;
CREATE TABLE date32_preimage (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO date32_preimage VALUES ('0000-01-01'), ('1500-06-15'), ('1899-12-31'), ('1993-01-15'), ('2299-12-31'), ('2300-01-01'), ('9000-12-31'), ('9999-12-31');

SELECT 'toYear is rewritten to a range for years outside the old 1900..2299 window';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 0 SETTINGS enable_analyzer=1;
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 1500 SETTINGS enable_analyzer=1;
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 9000 SETTINGS enable_analyzer=1;

SELECT 'toYear of the last representable year is not rewritten (the end of its preimage is not representable)';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYear(d) = 9999 SETTINGS enable_analyzer=1;

SELECT 'toYYYYMM is rewritten to a range for months outside the old window';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 150006 SETTINGS enable_analyzer=1;
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 900012 SETTINGS enable_analyzer=1;
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 999911 SETTINGS enable_analyzer=1;

SELECT 'toYYYYMM of the last representable month is not rewritten';
EXPLAIN QUERY TREE run_passes=1 SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 999912 SETTINGS enable_analyzer=1;

SELECT 'the results are correct both for the rewritten and for the non-rewritten predicates';
SELECT count() FROM date32_preimage WHERE toYear(d) = 0;
SELECT count() FROM date32_preimage WHERE toYear(d) = 1500;
SELECT count() FROM date32_preimage WHERE toYear(d) = 1993;
SELECT count() FROM date32_preimage WHERE toYear(d) = 9000;
SELECT count() FROM date32_preimage WHERE toYear(d) = 9999;
SELECT count() FROM date32_preimage WHERE toYear(d) = 1499;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 150006;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 900012;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 999912;
SELECT count() FROM date32_preimage WHERE toYYYYMM(d) = 999911;

DROP TABLE date32_preimage;
