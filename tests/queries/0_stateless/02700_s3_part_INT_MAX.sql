-- Tags: no-parallel, long

-- Regression test for crash in case of part exceeds INT_MAX
INSERT INTO FUNCTION s3('http://localhost:11111/test/test_INT_MAX.tsv', '', '', 'TSV')
-- NOTE: 2GiB is enough, but let's use 3GiB, just in case.
SELECT repeat('a', 1024) FROM numbers((pow(2, 30) * 3) / 1024)
SETTINGS s3_truncate_on_insert = 1, s3_max_single_part_upload_size = '10Gi';

SELECT count() FROM s3('http://localhost:11111/test/test_INT_MAX.tsv');
